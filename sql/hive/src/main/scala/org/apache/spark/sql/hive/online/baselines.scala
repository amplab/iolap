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

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, If, Row}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{Filter, Project, SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.online.OnlinePlannerUtil._

class BaselineDataFrame(dataFrame: DataFrame) extends OnlineDataFrame(dataFrame) {
  override protected[this] val olPlanner = new BaselineSQLPlanner(conf, this)
}

class BaselineSQLPlanner(conf: OnlineSQLConf, controller: OnlineDataFrame)
  extends OnlineSQLPlanner(conf, controller) {
  override protected val batches =
    Batch("Identify Sampled Relations", Once,
      SafetyCheck,
      IdentifyStreamedRelations(conf.streamedRelations, controller)
    ) ::
    Batch("Pre-Bootstrap Optimization", FixedPoint(100),
      PruneProjects
    ) ::
    Batch("Bootstrap", Once,
      PushUpResample,
      PushUpSeed,
      ImplementResample,
      PropagateBootstrap,
      BootstrapFilter,
      CleanupOutputTuples,
      CollectResult(conf.isDebug, conf.estimationConfidence)
    ) ::
    Batch("Post-Bootstrap Optimization", FixedPoint(100),
      PruneColumns,
      PushDownFilter,
      PruneProjects,
      OptimizeOperatorOrder,
      PruneFilters
    ) ::
    Batch("Consolidate Bootstrap", Once,
      ConsolidateBootstrap(conf.numBootstrapTrials)
    ) ::
    Batch("Materialize Plan", Once,
        CleanupAnalysisExpressions,
        ReplaceStreamedRelation
    ) ::
    Nil
}

object BootstrapFilter extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Filter(condition, child) if containsUncertain(condition.references) =>
      val projectList = child.output.map {
        case a@TaggedAttribute(t: Bootstrap, _, _, _, _) =>
          TaggedAlias(t, If(condition, a, byte0), "_btcnt")(a.exprId, a.qualifiers)
        case a => a
      }
      val project = Project(projectList, child)
      Filter(ExistsPlaceholder(ByteNonZero(getBtCnt(project.output).get)), project)
  }
}

case class CollectResult(isDebug: Boolean, confidence: Double) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val projectList = getBtCnt(plan.output) match {
      case Some(btCnt) =>
        plan.output.filter {
          case TaggedAttribute(Flag, _, _, _, _) => false
          case TaggedAttribute(_: Bound, _, _, _, _) => false
          case TaggedAttribute(_: Bootstrap, _, _, _, _) => false
          case _ => true
        }.map {
          case a: TaggedAttribute if !isDebug =>
            Alias(ApproxColumn(confidence, a :: Nil, btCnt :: Nil), a.name)(a.exprId, a.qualifiers)
          case a: Attribute => a
        }
      case None => plan.output
    }
    Project(projectList, plan)
  }
}

object ReplaceStreamedRelation extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case stream@StreamedRelation(child) =>
      AccStreamedRelation(child)(stream.reference, stream.controller, stream.trace)

    case OTStreamedRelation(child) => child
  }
}

case class AccStreamedRelation(child: SparkPlan)(
    val reference: RelationReference,
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil)
  extends UnaryNode with Stateful {
  override def output: Seq[Attribute] = child.output

  def getPartitions = trace.map(bId => controller.getPartitions(reference, bId)).reduce(_ ++ _)

  override def doExecute(): RDD[Row] =
    PartitionPruningRDD.create(child.execute(), getPartitions.contains)

  override protected final def otherCopyArgs = reference :: controller :: trace :: Nil

  override def simpleString = s"${super.simpleString} $trace"

  override def newBatch(newTrace: List[Int]): SparkPlan = {
    reference.setProcessed(
      newTrace.map(bId => controller.getPartitions(reference, bId)).map(_.size).sum)
    AccStreamedRelation(child)(reference, controller, newTrace)
  }
}
