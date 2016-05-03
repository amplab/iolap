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

package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.SQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLConf._

/**
 * Usage:
 * 1. Generate a TPC-H dataset and load it into hdfs.
 * 2. Use "create [tpch-dir]" to create external hive tables using the pre-loaded TPC-H dataset.
 * 3. Use "parquet [parquet-output-dir]" to generate preprocessed parquet dataset.
 *  This is required for now, as:
 *  (1) SparkSQL can only collect statistics for parquet relations,
 *    and statistics are very important for join optimization.
 *  (2) Preprocessing will insert a "__seed__" column for bootstrap,
 *    which is required for a very important optimization and hard-coded in the planner.
 *    (TODO: Make this optional)
 * 4. "drop" the previously created external tables.
 * 5. Use "create-parquet [parquet-output-dir]" to create external hive tables
 *  using the preprocessed parquet data.
 */
object TPCHSuite extends {
  val appName = "TPC-H Suite"
  val suiteName = "tpch"
} with SuiteHarness {

  override def setConf(queryName: String, debug: Boolean = false) = {
    queryName match {
      case "Q11" => sqlContext.setConf(STREAMED_RELATIONS, "partsupp")
      case "Q16" => sqlContext.setConf(STREAMED_RELATIONS, "partsupp")
      case "Q22" => sqlContext.setConf(STREAMED_RELATIONS, "customer")
      case _ => sqlContext.setConf(STREAMED_RELATIONS, "lineitem")
    }
    queryName match {
      case "Q18" =>
        sqlContext.setConf(SHUFFLE_PARTITIONS, (sparkContext.defaultParallelism * 2).toString)
      case _ =>
    }
    super.setConf(queryName, debug)
  }

  override def setTestConf(queryName: String): Unit = {
    queryName match {
      case "Q11" => sqlContext.setConf(STREAMED_RELATIONS, "partsupp_test")
      case "Q16" => sqlContext.setConf(STREAMED_RELATIONS, "partsupp_test")
      case "Q22" => sqlContext.setConf(STREAMED_RELATIONS, "customer_test")
      case _ => sqlContext.setConf(STREAMED_RELATIONS, "lineitem_test")
    }
    super.setTestConf(queryName)
  }
}
