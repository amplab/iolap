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

import java.util.NoSuchElementException

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.hive.online._
import org.apache.spark.storage.OLABlockId
import org.apache.spark.util.collection.CompactBuffer

@DeveloperApi
sealed abstract class CacheSide

@DeveloperApi
case object CacheRight extends CacheSide

@DeveloperApi
case object CacheLeft extends CacheSide

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class OTSortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    cacheSide: CacheSide,
    left: SparkPlan,
    right: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends BinaryNode with OTStateful {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  // this is to manually construct an ordering that can be used to compare keys from both sides
  private val keyOrdering: RowOrdering = RowOrdering.forSchema(leftKeys.map(_.dataType))

  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  @transient protected lazy val leftKeyGenerator = newProjection(leftKeys, left.output)
  @transient protected lazy val rightKeyGenerator = newProjection(rightKeys, right.output)

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] =
    keys.map(SortOrder(_, Ascending))

  def retrieveState(): RDD[Row] = prevBatch match {
    case Some(bId) =>
      val numParts = controller.olaBlocks(opId, bId)
      OLABlockRDD.create[Row](sparkContext, opId.id, Array((numParts, bId)), numParts)
    case None =>
      sys.error(s"Unexpected prevBatch = $prevBatch")
  }

  protected override def doExecute(): RDD[Row] = {
    val (leftResults, rightResults) = prevBatch match {
      case None =>
        val leftResults = left.execute().map(_.copy())
        val rightResults = right.execute().map(_.copy())

        cacheSide match {
          case CacheLeft =>
            controller.olaBlocks((opId, currentBatch)) = leftResults.partitions.length
            (
              leftResults.mapPartitionsWithIndex { case (index, iter) =>
                new AutoCachedIterator(OLABlockId(opId.id, currentBatch, index), iter)
              },
              rightResults
            )
          case CacheRight =>
            controller.olaBlocks((opId, currentBatch)) = rightResults.partitions.length
            (
              leftResults,
              rightResults.mapPartitionsWithIndex { case (index, iter) =>
                new AutoCachedIterator(OLABlockId(opId.id, currentBatch, index), iter)
              }
            )
        }
      case Some(_) =>
        cacheSide match {
          case CacheLeft => (retrieveState(), right.execute().map(_.copy()))
          case CacheRight => (left.execute().map(_.copy()), retrieveState())
        }
    }

    leftResults.zipPartitions(rightResults) { (leftIter, rightIter) =>
      new Iterator[Row] {
        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow5
        private[this] var leftElement: Row = _
        private[this] var rightElement: Row = _
        private[this] var leftKey: Row = _
        private[this] var rightKey: Row = _
        private[this] var rightMatches: CompactBuffer[Row] = _
        private[this] var rightPosition: Int = -1
        private[this] var stop: Boolean = false
        private[this] var matchKey: Row = _

        // initialize iterator
        initialize()

        override final def hasNext: Boolean = nextMatchingPair()

        override final def next(): Row = {
          if (hasNext) {
            // we are using the buffered right rows and run down left iterator
            val joinedRow = joinRow(leftElement, rightMatches(rightPosition))
            rightPosition += 1
            if (rightPosition >= rightMatches.size) {
              rightPosition = 0
              fetchLeft()
              if (leftElement == null || keyOrdering.compare(leftKey, matchKey) != 0) {
                stop = false
                rightMatches = null
              }
            }
            joinedRow
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        private def fetchLeft() = {
          if (leftIter.hasNext) {
            leftElement = leftIter.next()
            leftKey = leftKeyGenerator(leftElement)
          } else {
            leftElement = null
          }
        }

        private def fetchRight() = {
          if (rightIter.hasNext) {
            rightElement = rightIter.next()
            rightKey = rightKeyGenerator(rightElement)
          } else {
            rightElement = null
          }
        }

        private def initialize() = {
          fetchLeft()
          fetchRight()
        }

        /**
         * Searches the right iterator for the next rows that have matches in left side, and store
         * them in a buffer.
         *
         * @return true if the search is successful, and false if the right iterator runs out of
         *         tuples.
         */
        private def nextMatchingPair(): Boolean = {
          if (!stop && rightElement != null) {
            // run both side to get the first match pair
            while (!stop && leftElement != null && rightElement != null) {
              val comparing = keyOrdering.compare(leftKey, rightKey)
              // for inner join, we need to filter those null keys
              stop = comparing == 0 && !leftKey.anyNull
              if (comparing > 0 || rightKey.anyNull) {
                fetchRight()
              } else if (comparing < 0 || leftKey.anyNull) {
                fetchLeft()
              }
            }
            rightMatches = new CompactBuffer[Row]()
            if (stop) {
              stop = false
              // iterate the right side to buffer all rows that matches
              // as the records should be ordered, exit when we meet the first that not match
              while (!stop && rightElement != null) {
                rightMatches += rightElement
                fetchRight()
                stop = keyOrdering.compare(leftKey, rightKey) != 0
              }
              if (rightMatches.size > 0) {
                rightPosition = 0
                matchKey = leftKey
              }
            }
          }
          if (rightMatches != null && rightMatches.size > 0) {
            true
          } else {
            cacheSide match {
              case CacheLeft => while (leftElement != null) fetchLeft()
              case CacheRight => while (rightElement != null) fetchRight()
            }
            false
          }
        }
      }
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan =
    OTSortMergeJoin(leftKeys, rightKeys, cacheSide, left, right)(controller, newTrace, opId)
}
