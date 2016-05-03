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

package org.apache.spark.sql.hive.online.joins

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.hive.online._
import org.apache.spark.util.collection.CompactBuffer2

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.existentials

/**
 * Multi-time broadcast hash join.
 * Remarks:
 *  1. Now we only handle the case with AlmostFixed build side.
 *    We assume "almost-fixed" means that the first iteration will see all the keys.
 *    Therefore, we have the following remarks.
 *  2. We keep track of all the keys with at least one false flag from the build side,
 *    and throw exception if we see new keys out of this set in later iterations.
 *  3. We cache from the stream side the tuples with true flags
 *    but joined with at least one false-flagged build-side tuple.
 *    Stream-side cache is saved in state.
 *  4. Build-side cache is saved in broadcast.
 *  5. We refresh both build-side and stream-side caches.
 */
case class MTBroadcastHashJoin(
    leftCacheFilter: Option[Attribute],
    rightCacheFilter: Option[Attribute],
    streamRefresh: RefreshInfo,
    buildRefresh: RefreshInfo,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends BinaryNode with HashJoin with Stateful
  with IteratorRefresher with HashedRelationRefresher {

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  protected val (buildCacheFilter, streamedCacheFilter) = buildSide match {
    case BuildLeft => (leftCacheFilter, rightCacheFilter)
    case BuildRight => (rightCacheFilter, leftCacheFilter)
  }

  protected def hashJoin2(
    streamIter: Iterator[Row],
    hashedRelation: HashedRelation2): Iterator[Row] = {

    new Iterator[Row] {
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: CompactBuffer2[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow2

      private[this] val joinKeys = streamSideKeyGenerator()

      override final def hasNext: Boolean =
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

      override final def next() = {
        val ret = buildSide match {
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        ret
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashedRelation.get(joinKeys.currentValue)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }
  }

  val timeout = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private lazy val broadcastFuture = future {
    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    val input: Array[Row] = buildPlan.execute().map(_.copy()).collect()
    val predicate = buildCacheFilter match {
      case Some(filter) => newPredicate(filter, buildPlan.output)
      case None => (_: Row) => true
    }
    val hashed = HashedRelation2(input.iterator, buildSideKeyGenerator, predicate, input.length)
    prevBatches.lastOption match {
      case None =>
        val broadcast = sparkContext.broadcast(
          CombinedHashedRelation(hashed, null, refreshHashedRelation))
        controller.broadcasts((opId, currentBatch)) = broadcast
        broadcast
      case Some(bId) =>
        // TODO: fix this integrity error by supporting join whose both branches may grow
        if (!hashed.keySet().isEmpty) {
          controller.getWatcher += -1
          logError(s"Integrity Error in MTBroadcastHashJoin(Op $opId, Batch $currentBatch)")
        }
        controller.broadcasts((opId, bId)).asInstanceOf[Broadcast[HashedRelation2]]
    }
  }(BroadcastHashJoin.broadcastHashJoinExecutionContext)

  override def doExecute() = {
    val broadcastRelation = Await.result(broadcastFuture, timeout)
    streamedPlan.execute().mapPartitions { streamedIter =>
      new Iterator[Row] {
        private[this] val iterator =
          hashJoin2(streamedIter, refreshHashedRelation()(broadcastRelation.value))

        override def hasNext: Boolean = iterator.hasNext

        override def next(): Row = iterator.next()
      }
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan = {
    val join = MTBroadcastHashJoin(
      leftCacheFilter, rightCacheFilter, streamRefresh, buildRefresh,
      leftKeys, rightKeys, buildSide, left, right)(controller, newTrace, opId)
    join.broadcastFuture
    join
  }
}
