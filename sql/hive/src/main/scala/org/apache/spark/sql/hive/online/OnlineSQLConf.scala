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

import org.apache.spark.sql.SQLContext

object OnlineSQLConf {
  val NUMBER_BOOTSTRAP_TRIALS = "spark.sql.online.number.bootstrap.trials"
  val STREAMED_RELATIONS = "spark.sql.online.streamed.relations"
  val SLACK_PARAM = "spark.sql.online.slack"
  val NUMBER_BATCHES = "spark.sql.online.number.batches"
  val NUM_WAVE_PER_BATCH = "spark.sql.online.number.wave.per.batch"

  val SEED_COLUMN = "__seed__"

  val DEBUG_FLAG = "spark.sql.online.debug"
  val ESTIMATION_CONFIDENCE = "spark.sql.online.estimation.confidence"
  val NUMBER_TAKE_BATCHES = "spark.sql.online.take.batches"
}

class OnlineSQLConf(sqlContext: SQLContext) {
  import OnlineSQLConf._

  private[spark] def numBootstrapTrials: Int = {
    val numTrials = sqlContext.getConf(NUMBER_BOOTSTRAP_TRIALS, "100").toInt
    assert(numTrials > 0, "Number of bootstrap trials must be a positive integer")
    numTrials
  }

  private[spark] def streamedRelations: Set[String] = sqlContext.getConf(STREAMED_RELATIONS, "")
    .split(",").map(_.trim).filter(_.nonEmpty).toSet

  private[spark] def numBatches: Option[Int] =
    Option(sqlContext.getConf(NUMBER_BATCHES, null)).map(_.toInt)

  private[spark] def numWavesPerBatch: Double = sqlContext.getConf(NUM_WAVE_PER_BATCH, "1").toDouble

  private[spark] def takeBatches: Int =
    sqlContext.getConf(NUMBER_TAKE_BATCHES, Int.MaxValue.toString).toInt

  private[spark] def variationRangeSlack: Double = sqlContext.getConf(SLACK_PARAM, "2.0").toDouble

  private[spark] def isDebug: Boolean = sqlContext.getConf(DEBUG_FLAG, "false").toBoolean

  private[spark] def estimationConfidence: Double =
    sqlContext.getConf(ESTIMATION_CONFIDENCE, "0.95").toDouble
}
