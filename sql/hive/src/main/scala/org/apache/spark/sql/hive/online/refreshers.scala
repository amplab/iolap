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

import java.util.{HashMap => JHashMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.online.joins.{HashedRelation2, MutableIterator}
import org.apache.spark.storage.{StorageLevel, LazyBlockId}
import org.apache.spark.{HashPartitioner, SparkEnv}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

case class RefreshInfo(
    lazyEvals: Seq[Expression],
    dependencies: Seq[(OpId, (Seq[Attribute], Seq[NamedExpression], Int))],
    inputSchema: Seq[Attribute]) {

  def extendedSchema = inputSchema ++ dependencies.flatMap(_._2._1)

  override def toString = "Refresh(" +
    lazyEvals.mkString("[", ", ", "]") + ", " +
    dependencies.map { case (opId, triple) =>
      s"$opId -> (${triple._1.mkString("[", ", ", "]")}, " +
        s"${triple._2.mkString("[", ", ", "]")}, ${triple._3})"
    }.mkString("[", ", ", "]") + ", " +
    inputSchema.mkString("[", ", ", "]") + ")"
}

/**
 * Remark: The blocks are not available until we first call MapGet.eval
 */
case class UpdatesHashMap(opId: OpId, numPartitions: Int, batchId: Int) {
  private[this] val partitioner = new HashPartitioner(numPartitions)
  private[this] val maps: Array[JHashMap[Row, Row]] = new Array[JHashMap[Row, Row]](numPartitions)
  private[this] var numActiveMaps = 0

  def apply(key: Row): Row = {
    val index = partitioner.getPartition(key)
    if (maps(index) == null) {
      val blockManager = SparkEnv.get.blockManager
      val lazyBlockId = LazyBlockId(opId.id, batchId, index)
      numActiveMaps += 1
      maps(index) =
        blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY) match {
          case Some(data) => data.asInstanceOf[JHashMap[Row, Row]]
          case None => throw new Exception(s"Could not find $lazyBlockId")
        }
      if (numActiveMaps > 1) {
        (0 until numPartitions).foreach { idx =>
          if (maps(idx) == null) future {
            val lazyBlockId = LazyBlockId(opId.id, batchId, idx)
            blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY)
          }
        }
      }
    }
    maps(index).get(key)
  }

  override def toString = maps.flatMap(_.iterator).mkString("[", ",", "]")
}

trait Refresher {
  self: SparkPlan with Stateful =>

  def apply(iterator: Iterator[Row], refreshInfo: RefreshInfo): Iterator[Row] = {
    import refreshInfo._

    if (iterator.nonEmpty) {
      dependencies.foldLeft(iterator) { (iter, dependency) =>
        dependency._2._2 match {
          case Seq() =>
            val opId = dependency._1
            val blockManager = SparkEnv.get.blockManager
            val lazyBlockId = LazyBlockId(opId.id, currentBatch, 0)
            val row = blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY) match {
              case Some(data) => data.asInstanceOf[Row]
              case None => throw new Exception(s"Could not find $lazyBlockId")
            }
            cartesianJoin(iter, row)
          case keys =>
            val joinKeys = newMutableProjection(keys, inputSchema)()
            val updates = UpdatesHashMap(dependency._1, dependency._2._3, currentBatch)
            hashJoin(iter, joinKeys, updates)
        }
      }
    } else iterator
  }

  def hashJoin(
    iterator: Iterator[Row],
    joinKeys: MutableProjection,
    updates: UpdatesHashMap): Iterator[Row] = {
    new Iterator[Row] {
      private[this] val joinRow = new JoinedRow2

      override final def hasNext: Boolean = iterator.hasNext

      override final def next() = {
        val currentRow = iterator.next()
        joinRow(currentRow, updates(joinKeys(currentRow)))
      }
    }
  }

  def cartesianJoin(iterator: Iterator[Row], update: Row): Iterator[Row] = {
    new Iterator[Row] {
      private[this] val joinRow = new JoinedRow2(null, update)

      override final def hasNext: Boolean = iterator.hasNext

      override final def next() = joinRow.withLeft(iterator.next())
    }
  }
}

trait IteratorRefresher extends Refresher {
  self: SparkPlan with Stateful =>

  val streamRefresh: RefreshInfo

  def refresh(rdd: RDD[Row]): RDD[Row] = {
    import streamRefresh._

    lazyEvals match {
      case Seq() => rdd
      case _ => prevBatch match {
        case None => rdd
        case Some(_) =>
          rdd.mapPartitions { iterator =>
            val projection = newMutableProjection(lazyEvals, extendedSchema)()
            this(iterator, streamRefresh).map(projection)
          }
      }
    }
  }
}

trait HashedRelationRefresher extends Refresher {
  self: SparkPlan with Stateful =>

  val buildRefresh: RefreshInfo

  def refreshHashedRelation(): HashedRelation2 => HashedRelation2 = {
    import buildRefresh._

    lazyEvals match {
      case Seq() => (hashedRelation: HashedRelation2) => hashedRelation
      case _ => prevBatch match {
        case None =>
          (hashedRelation: HashedRelation2) => hashedRelation
        case Some(_) =>
          (hashedRelation: HashedRelation2) => {
            val iterator: MutableIterator[Row] = hashedRelation.updatableIterator()
            val projection = newProjection(lazyEvals, extendedSchema)
            this(iterator, buildRefresh).foreach(row => iterator.update(projection(row)))
            hashedRelation
          }
      }
    }
  }
}
