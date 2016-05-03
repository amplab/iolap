/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.online

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.online.Growth._
import org.apache.spark.storage.OLABlockId

case class RelationReference(id: Int, numParts: Int) {
  private[this] var numReferences: Int = 0
  def reference(): Unit = numReferences += 1
  def multiRef: Boolean = numReferences > 1

  def setProcessed(processed: Double): Unit = _scale = numParts / processed
  private[this] var _scale: Double = 0
  def scale: Double = _scale
}

object RelationReference {
  private[this] val curId = new AtomicInteger()
  def newReference(numParts: Int) = RelationReference(curId.getAndIncrement, numParts)
}

case class OpId(id: Int) {
  override def toString = s"<$id>"
}

object OpId {
  private[this] val curId = new AtomicInteger()
  def newOpId = OpId(curId.getAndIncrement)
}

trait Stateful {
  self: SparkPlan =>

  val controller: OnlineDataFrame
  val trace: List[Int]

  val currentBatch = trace.head
  val prevBatch: Option[Int] = trace.tail.headOption
  def prevBatches: Seq[Int] = trace.tail

  override def equals(other: Any): Boolean = other match {
    case s: Stateful => super.equals(s) && trace == s.trace
    case _ => false
  }

  def newBatch(newTrace: List[Int]): SparkPlan
}

trait OTStateful extends Stateful {
  self: SparkPlan =>

  override val prevBatch = trace.tail.lastOption
  override val prevBatches = prevBatch.toSeq
}

case class OTStreamedRelation(child: SparkPlan)(
  @transient val controller: OnlineDataFrame,
  @transient val trace: List[Int] = -1 :: Nil)
  extends UnaryNode with OTStateful {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[Row] = prevBatch match {
    case None => child.execute()
    case Some(bId) => new EmptyIteratorRDD(sparkContext, child.execute().partitions.length)
  }

  override protected final def otherCopyArgs = controller :: trace :: Nil

  override def simpleString = s"${super.simpleString} $trace"

  override def newBatch(newTrace: List[Int]): SparkPlan =
    OTStreamedRelation(child)(controller, newTrace)
}

case class StreamedRelation(child: SparkPlan)(
    val reference: RelationReference,
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil)
  extends UnaryNode with Stateful {
  override def output: Seq[Attribute] = child.output

  def getPartitions = controller.getPartitions(reference, currentBatch)

  override def doExecute(): RDD[Row] =
    PartitionPruningRDD.create(child.execute(), getPartitions.contains)

  override protected final def otherCopyArgs = reference :: controller :: trace :: Nil

  override def simpleString = s"${super.simpleString} $trace"

  override def newBatch(newTrace: List[Int]): SparkPlan = {
    reference.setProcessed(
      newTrace.map(bId => controller.getPartitions(reference, bId)).map(_.size).sum)
    StreamedRelation(child)(reference, controller, newTrace)
  }
}

case class Collect(
    cacheFilter: Option[Attribute],
    streamRefresh: RefreshInfo,
    projectList: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends UnaryNode with IteratorRefresher with Stateful {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def requiredChildDistribution = AllTuples :: Nil

  def cache(iterator: Iterator[Row]): CachedIterator = cacheFilter match {
    case Some(filter) =>
      new SelectiveAutoCachedIterator(
        newPredicate(filter, child.output), OLABlockId(opId.id, currentBatch, 0), iterator)
    case None => new AutoCachedIterator(OLABlockId(opId.id, currentBatch, 0), iterator)
  }

  override val prevBatch = trace.tail.headOption
  override val prevBatches = trace.tail

  def retrieveState(): RDD[Row] = {
    val batchSizes = prevBatches.map(bId => (controller.olaBlocks(opId, bId), bId)).toArray
    val cached = OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, 1)
    refresh(cached)
  }

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  override def doExecute(): RDD[Row] = prevBatch match {
    case Some(_) =>
      val rdd = child.execute()
      controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length
      rdd.zipPartitions(retrieveState()) {
        (iterator1, iterator2) => iterator2 ++ cache(iterator1)
      }.mapPartitions { iterator =>
        val reusableProjection = buildProjection()
        iterator.map(reusableProjection)
      }
    case None =>
      val rdd = child.execute()
      controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length
      rdd.mapPartitions { iterator =>
        val reusableProjection = buildProjection()
        cache(iterator).map(reusableProjection)
      }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan =
    Collect(cacheFilter, streamRefresh, projectList, child)(controller, newTrace, opId)
}

case class OnlineProject(
    streamRefresh: RefreshInfo,
    cacheInfo: (Int, Int),
    fixed: Boolean,
    projectList: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends UnaryNode with IteratorRefresher with Stateful {

  override def output = projectList.map(_.toAttribute)

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  def buildCacheChecker(): (Row, Row) => Boolean = cacheInfo match {
    case (-1, oi) => (_: Row, row: Row) => !row.getBoolean(oi)
    case (ii, oi) => (row1: Row, row2: Row) => row1.getBoolean(ii) != row2.getBoolean(oi)
  }

  def withState(rdd: RDD[Row]) = prevBatch match {
    case Some(bId) =>
      val batchSizes = Array((controller.olaBlocks((opId, bId)), bId))
      val cached =
        if (fixed) OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd.partitions.length)
        else OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd)
      rdd.zipPartitions(refresh(cached)) { (iterator1, iterator2) => iterator1 ++ iterator2 }
    case None => rdd
  }

  override def doExecute() = {
    val rdd = child.execute()
    controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length

    withState(rdd).mapPartitionsWithIndex { (index, iterator) =>
      new AutoCachedIterator(OLABlockId(opId.id, currentBatch, index), iterator) {
        private[this] val buffer: MutableRow = new GenericMutableRow(projectList.length)
        private[this] val reusableProjection = buildProjection().target(buffer)

        val cacheChecker = buildCacheChecker()

        override final def next(): Row = {
          val row = iterator.next()
          reusableProjection(row)
          if (cacheChecker(row, buffer)) {
            cache += row.copy()
          }
          buffer
        }
      }
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan =
    OnlineProject(streamRefresh, cacheInfo, fixed, projectList, child)(controller, newTrace, opId)
}
