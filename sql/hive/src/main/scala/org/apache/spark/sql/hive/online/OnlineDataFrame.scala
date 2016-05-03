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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.rules.{RuleExecutor, Rule}
import org.apache.spark.sql.execution.joins.{SortMergeJoin, ShuffledHashJoin, BroadcastHashJoin}
import org.apache.spark.sql.{KickOffBroadcast, Row, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.online.OnlineDataFrame._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{LazyBlockId, OLABlockId}
import org.apache.spark.{Accumulator, AccumulatorParam, SparkEnv}

import scala.collection.mutable
import scala.util.Random

class OnlineDataFrame(dataFrame: DataFrame) extends org.apache.spark.Logging {
  private[this] val sqlContext = dataFrame.sqlContext
  private[this] val sparkContext = sqlContext.sparkContext

  private[this] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once,
        EnsureRequirements(sqlContext),
        PushUpOTStreamedRelation,
        KickOffBroadcast
      ) :: Nil
  }

  protected[this] val conf = new OnlineSQLConf(sqlContext)
  private[this] val numBatches: Option[Int] = conf.numBatches
  private[this] val numWavesPerBatch: Double = conf.numWavesPerBatch
  private[this] val takeBatches: Int = conf.takeBatches
  private[this] val numSlots: Int = sparkContext.defaultParallelism

  protected[this] val olPlanner = new OnlineSQLPlanner(conf, this)

  val olaBlocks = new mutable.HashMap[(OpId, Int), Int]()
  val lazyBlocks = new mutable.HashMap[(OpId, Int), Int]()
  val broadcasts = new mutable.HashMap[(OpId, Int), Broadcast[_]]()

  private[this] val relation2Batches =
    new mutable.HashMap[RelationReference, mutable.Buffer[Set[Int]]]()

  def registerRelation(ref: RelationReference): Unit = {
    val batchSize = numBatches.map(n => math.max(ref.numParts / n, 1))
      .getOrElse(math.ceil(numSlots * numWavesPerBatch).toInt)
    val partitions = Random.shuffle((0 until ref.numParts).toIndexedSeq)
      .grouped(batchSize).map(_.toSet).toBuffer
    relation2Batches(ref) = partitions
    activeNumBatches = math.max(activeNumBatches, partitions.length)
  }

  def getPartitions(ref: RelationReference, batchId: Int): Set[Int] =
    relation2Batches.get(ref).map {
      parts => if (batchId < parts.length) parts(batchId) else Set[Int]()
    }.getOrElse(Set[Int]())

  private[this] var activeNumBatches = 1
  private[this] var watcher: Accumulator[Int] = null
  private[this] var batches: List[Int] = Nil

  // extract the spark plan from the input data frame for rewriting
  lazy val executedPlan =
    prepareForExecution.execute(olPlanner.execute(dataFrame.queryExecution.sparkPlan))

  lazy val schema: StructType = executedPlan.schema

  def progress: (Int, Int) = (batches.length, activeNumBatches)

  def hasNext: Boolean = {
    executedPlan
    val active = batches.headOption.forall(_ + 1 < math.min(activeNumBatches, takeBatches))
    if (!active) {
      cleanup(_ => true)
    }
    active
  }

  def collectNext(): Array[Row] = {
    var rows: Array[Row] = null
    do {
      rows = next().collect()
    } while(!isValid)

    rows
  }

  private[spark] def next(): DataFrame = {
    batches.headOption match {
      case Some(bId) =>
        if (bId != watcher.value) {
          val (toRecompute, rest) = batches.span(_ != watcher.value)
          batches = rest
          logWarning(s"Recomputing batches ${toRecompute.mkString("[", ",", "]")}")
          recompute(toRecompute, bId + 1)
        }
        batches = (bId + 1) :: batches
      case None =>
        batches = 0 :: batches
    }
    watcher = new Accumulator(batches.head, MinAccumulatorParam)

    sqlContext.createDataFrame(generate(executedPlan, batches).execute(), schema)
  }

  private[spark] def isValid: Boolean = batches.headOption.exists(_ == watcher.value)

  def cleanup(): Unit = cleanup(_ => true)

  private[this] def generate(plan: SparkPlan, batches: List[Int]): SparkPlan = plan.transformUp {
    case stateful: Stateful =>
      stateful.transformAllExpressions {
        case ScaleFactor(branches) => Literal(branches.map(_.scale).product)
        case ApproxColumn(confidence, column, multiplicities, _)
          if batches.headOption.forall(_ + 1 == activeNumBatches) =>
          ApproxColumn(confidence, column, multiplicities, finalBatch = true)
      }.newBatch(batches)

    case other =>
      other.transformAllExpressions {
        case ScaleFactor(branches) => Literal(branches.map(_.scale).product)
        case ApproxColumn(confidence, column, multiplicities, _)
          if batches.headOption.forall(_ + 1 == activeNumBatches) =>
          ApproxColumn(confidence, column, multiplicities, finalBatch = true)
      }
  }

  private[this] def recompute(toRecompute: Seq[Int], index: Int): Unit = {
    // clean up
    val toCleanup = toRecompute.toSet
    cleanup(toCleanup.contains)

    relation2Batches.values.foreach { parts =>
      parts.insert(index, toRecompute.map(parts).reduce(_ ++ _))
      activeNumBatches = math.max(activeNumBatches, parts.length)
    }
  }

  private[this] def cleanup(p: Int => Boolean): Unit = {
    // clean up
    val env = SparkEnv.get
    olaBlocks.retain {
      case ((opId, bId), size) if p(bId) =>
        (0 until size).foreach { i =>
          env.blockManager.master.removeBlock(OLABlockId(opId.id, bId, i))
        }
        false
      case _ => true
    }
    lazyBlocks.retain {
      case ((opId, bId), size) if p(bId) =>
        (0 until size).foreach { i =>
          env.blockManager.master.removeBlock(LazyBlockId(opId.id, bId, i))
        }
        false
      case _ => true
    }
    broadcasts.retain {
      case ((opId, bId), broadcast) if p(bId) =>
        env.broadcastManager.unbroadcast(broadcast.id, removeFromDriver = true, blocking = false)
        false
      case _ => true
    }
  }

//  private[this] def statusString(): String = {
//    relation2Batches.map {
//      case (ref, parts) => s"$ref -> ${parts.mkString("[", ",", "]")}"
//    }.mkString(", ")
//  }

  def getWatcher: Accumulator[Int] = watcher
}

object OnlineDataFrame {

  implicit object MinAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = math.min(t1, t2)
    def zero(initialValue: Int) = initialValue
  }
}

object PushUpOTStreamedRelation extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Project(projectList, ot@OTStreamedRelation(child)) =>
      OTStreamedRelation(Project(projectList, child))(ot.controller)

    case Filter(condition, ot@OTStreamedRelation(child)) =>
      OTStreamedRelation(Filter(condition, child))(ot.controller)

    case Aggregate(partial, groupings, aggrs, ot@OTStreamedRelation(child)) =>
      OTStreamedRelation(Aggregate(partial, groupings, aggrs, child))(ot.controller)

    case Exchange(partitioning, ordering, ot@OTStreamedRelation(child)) =>
      OTStreamedRelation(Exchange(partitioning, ordering, child))(ot.controller)

    case BroadcastHashJoin(leftKeys, rightKeys, buildSide,
      ot@OTStreamedRelation(left), OTStreamedRelation(right)) =>
      OTStreamedRelation(
        BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right)
      )(ot.controller)

    case ShuffledHashJoin(leftKeys, rightKeys, buildSide,
      ot@OTStreamedRelation(left), OTStreamedRelation(right)) =>
      OTStreamedRelation(
        ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right)
      )(ot.controller)

    case SortMergeJoin(leftKeys, rightKeys,
      ot@OTStreamedRelation(left), OTStreamedRelation(right)) =>
      OTStreamedRelation(SortMergeJoin(leftKeys, rightKeys, left, right))(ot.controller)
  }
}
