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

import java.util.{HashSet => JHashSet}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Expression, MutableProjection, Row}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.joins.{BuildRight, HashJoin}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.hive.online.{OTStateful, OnlineDataFrame, OpId}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

/**
 * One-time broadcast implementation of left semi hash join.
 */
case class OTBLeftSemiHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends BinaryNode with HashJoin with OTStateful {

  override val buildSide = BuildRight

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  override def output = left.output

  @transient private[this] lazy val keyGenerator: () => MutableProjection =
    newMutableProjection(buildKeys, buildPlan.output)

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
    prevBatch match {
      case None =>
        // Note that we use .execute().collect() because we don't want to convert data to Scala types
        val input: Array[Row] = buildPlan.execute()
          .mapPartitions(HashedSet(_, keyGenerator())).collect()
        val hashed = HashedSet(input.iterator)
        val broadcast = sparkContext.broadcast(hashed)
        controller.broadcasts((opId, currentBatch)) = broadcast
        broadcast
      case Some(bId) =>
        controller.broadcasts((opId, bId)).asInstanceOf[Broadcast[JHashSet[Row]]]
    }
  }

  override def doExecute() = {
    val broadcastRelation: Broadcast[JHashSet[Row]] = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamIter =>
      val hashSet = broadcastRelation.value
      val joinKeys = streamSideKeyGenerator()
      streamIter.filter(current => {
        !joinKeys(current).anyNull && hashSet.contains(joinKeys.currentValue)
      })
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan = {
    val join = OTBLeftSemiHashJoin(leftKeys, rightKeys, left, right)(controller, newTrace, opId)
    join.broadcastFuture
    join
  }
}
