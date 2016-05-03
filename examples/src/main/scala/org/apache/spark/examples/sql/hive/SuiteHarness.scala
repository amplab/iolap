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

import java.io.File

import org.apache.spark.sql.SQLConf._
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._
import org.apache.spark.sql.hive.online.RandomSeed
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

trait SuiteHarness {
  val appName: String

  val sparkConf = new SparkConf().setAppName(appName)
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)

  val masterUrl = sparkConf.getenv("SPARK_MASTER_IP")
  val hdfsUrl = s"hdfs://$masterUrl:9010"

  def main(args: Array[String]): Unit = {
    args(0) match {
      case PREPROC_CMD => preproc(args(1), args(2), args(3).toInt)

      case PARTITION_CMD => partition(args(1), args(2), args(3).toInt)

      case CREATE_CMD(option) =>
        option match {
          case "-parquet" => createAsParquet(args(1))
          case _ => createAsHive(args(1))
        }

      case DROP_CMD => drop()

      case SHOW_CMD => show()

      case STATS_CMD => stats()

      case TEST_CMD => test(args(1), args(2))

      case RUN_CMD(option) =>
        val queryName = args(1)
        setConf(queryName)
        
        val dataFrame = sqlContext.sql(queries(queryName))
        option match {
          case "-0" =>
            println(s"REMARK: Running $queryName baseline...")
            hiveRun(dataFrame)
          case "-1" =>
            println(s"REMARK: Running $queryName online single-batch...")
            sqlContext.setConf(NUMBER_BATCHES, "1")
            onlineRun(dataFrame)
          case "-2" =>
            println(s"REMARK: Running $queryName online multi-batch...")
            onlineRun(dataFrame)
          case "-3" =>
            println(s"REMARK: Running $queryName baseline multi-batch...")
            onlineRun(dataFrame)
        }

      case DEBUG_CMD(option) =>
        val queryName = args(1)
        val query = queries(queryName)

        setConf(queryName, debug = true)
        val plan = option match {
          case "-0" => sqlContext.sql(query).queryExecution.executedPlan
          case "-1" => sqlContext.sql(query).online.executedPlan
          case "-2" => sqlContext.sql(query).baseline.executedPlan
        }
        printPlan(plan)

      case other => dispatch(args)
    }
  }

  def dispatch(args: Array[String]): Unit = {  }

  def setConf(queryName: String, debug: Boolean = false): Unit = {
    sqlContext.setConf(SHUFFLE_PARTITIONS, sparkContext.defaultParallelism.toString)
    if (debug) sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, "2")
  }

  def setTestConf(queryName: String): Unit = {
    sqlContext.setConf(SHUFFLE_PARTITIONS, sparkContext.defaultParallelism.toString)
  }

  def hiveRun(dataFrame: DataFrame): Unit = {
    printResult(dataFrame, "baseline")
  }

  def onlineRun(dataFrame: DataFrame): Unit = {
    val query = dataFrame.online
    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), s"{batch = $batchId}")
      batchId += 1
    }
  }

  def printPlan(plan: SparkPlan): Unit = {
    println("=============Plan=================")
    println(plan)
    println("==================================")
  }

  def printResult(query: DataFrame, tag: String): Unit = {
    query.queryExecution.executedPlan // force to initialize the query plan

    var rows: Array[Row] = null
    benchmark {
      rows = query.collect()
      print(s"<<< $tag:\t")
    }

    println(s"==============$tag===============")
    rows.foreach(row => println("###:\t" + row.mkString(", ")))
    println("====================================")
  }

  def preproc(table: String, parquet: String, numParts: Int): Unit =
    sqlContext.table(table).withColumn(SEED_COLUMN, new Column(RandomSeed()))
      .repartition(numParts)
      .write.format("parquet").save(s"$hdfsUrl/$parquet/$table")

  def partition(table: String, parquet: String, numParts: Int): Unit =
    sqlContext.table(table).repartition(numParts)
      .write.format("parquet").save(s"$hdfsUrl/$parquet/$table")

  def createAsParquet(parquet: String): Unit = tables.keys.foreach {
    table => sqlContext.createExternalTable(table, s"$hdfsUrl/$parquet/$table", "parquet")
  }
  
  def createAsHive(hive: String): Unit = tables.foreach {
    case (table, ddl) =>
      sqlContext.sql(ddl.replace(":root", s"$hdfsUrl/$hive")).collect()
  }

  def show(): Unit = {
    val results = sqlContext.sql("show tables").collect()
    println("=====================")
    results.foreach(println(_))
    println("=====================")
  }

  def drop(): Unit = tables.keys.foreach { table => sqlContext.sql(s"drop table $table").collect() }

  def stats(): Unit = {
    val results = tables.keys.map { table =>
      val sizeInBytes =
        sqlContext.table(table).queryExecution.optimizedPlan.statistics.sizeInBytes.toLong
      table -> sizeInBytes
    }
    println("===========================")
    results.foreach { case (table, sizeInBytes) => println(s"size of $table = $sizeInBytes bytes") }
    println("===========================")
  }

  def test(queryName: String, parquet: String): Unit = {
    // REMARK: assume that HadoopRDD.getPartitions returns the same array of partitions every time.
    setTestConf(queryName)
    tables.keys.foreach { table =>
      sqlContext.read.parquet(s"$hdfsUrl/$parquet/$table").registerTempTable(s"${table}_test")
      val df = sqlContext.table(s"${table}_test")
      val cols = df.schema.fields.map { f =>
        val dataType = f.dataType match {
          case DoubleType | FloatType => DecimalType.Unlimited
          case t => t
        }
        df.col(f.name).cast(dataType).as(f.name)
      }
      df.select(cols: _*).registerTempTable(table)
    }

    println(s"REMARK: Testing $queryName...")

    val ol = sqlContext.sql(queries(queryName)).online
    val bl = sqlContext.sql(queries(queryName)).baseline
    var batchId = 0
    while (ol.hasNext && bl.hasNext) {
      val output: Array[Row] = ol.collectNext()
      val expected = bl.collectNext()

      val paired = output.zipAll(expected, EmptyRow, EmptyRow)
      println(s"==============batch $batchId===================")
      paired.foreach(p => println(p))
      println("================================================")
      paired.foreach(p => assert(p._1 == p._2, s"$queryName failed in batch $batchId due to $p"))

      batchId += 1
    }
  }

  val kit = Seq("/mnt", "/root").collectFirst {
    case root if new File(s"$root/benchmark-kit").exists() => s"$root/benchmark-kit/"
  }.getOrElse(sys.error("Cannot find benchmark-kit"))

  val suiteName: String

  val scale: Int = sqlContext.getConf("spark.sql.online.test.scale").toInt

  val queries = new File(s"$kit/$suiteName/query").listFiles().map {
    case hql => hql.getName -> Source.fromFile(hql).mkString.replace(":scale", s"$scale")
  }.toMap

  val tables = new File(s"$kit/$suiteName/ddl").listFiles().map {
    case ddl => ddl.getName -> Source.fromFile(ddl).mkString
  }.toMap

  val PREPROC_CMD = "preproc"
  val PARTITION_CMD = "partition"
  val CREATE_CMD = "create(.*)".r
  val DROP_CMD = "drop"
  val SHOW_CMD = "show"
  val STATS_CMD = "stats"
  val TEST_CMD = "test"
  val RUN_CMD = "run(.*)".r
  val DEBUG_CMD = "debug(.*)".r
}
