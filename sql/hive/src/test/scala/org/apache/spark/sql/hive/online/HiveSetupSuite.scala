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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class HiveSetupSuite extends FunSuite {
  val url = ""
  val master = s"spark://$url:7077"
  val hdfs = s"hdfs://$url:9010/tpch"

  val sparkContext = new SparkContext(master, "TestSQLContext",
    new SparkConf().set("spark.sql.test", "")
      .set("spark.executor.memory", "48g"))
  val hiveContext = new HiveContext(sparkContext)

  import hiveContext._

  test("create tables") {
    val ddls = Seq(
      sql(
        s"""
          |create external table lineitem (
          | L_ORDERKEY INT,
          | L_PARTKEY INT,
          | L_SUPPKEY INT,
          | L_LINENUMBER INT,
          | L_QUANTITY DOUBLE,
          | L_EXTENDEDPRICE DOUBLE,
          | L_DISCOUNT DOUBLE,
          | L_TAX DOUBLE,
          | L_RETURNFLAG STRING,
          | L_LINESTATUS STRING,
          | L_SHIPDATE STRING,
          | L_COMMITDATE STRING,
          | L_RECEIPTDATE STRING,
          | L_SHIPINSTRUCT STRING,
          | L_SHIPMODE STRING,
          | L_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/lineitem'
        """.stripMargin),
      sql(
        s"""
          |create external table orders (
          | O_ORDERKEY INT,
          | O_CUSTKEY INT,
          | O_ORDERSTATUS STRING,
          | O_TOTALPRICE DOUBLE,
          | O_ORDERDATE STRING,
          | O_ORDERPRIORITY STRING,
          | O_CLERK STRING,
          | O_SHIPPRIORITY INT,
          | O_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/orders'
        """.stripMargin),
      sql(
        s"""
          |create external table customer (
          | C_CUSTKEY INT,
          | C_NAME STRING,
          | C_ADDRESS STRING,
          | C_NATIONKEY INT,
          | C_PHONE STRING,
          | C_ACCTBAL DOUBLE,
          | C_MKTSEGMENT STRING,
          | C_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/customer'
        """.stripMargin),
      sql(
        s"""
          |create external table supplier (
          | S_SUPPKEY INT,
          | S_NAME STRING,
          | S_ADDRESS STRING,
          | S_NATIONKEY INT,
          | S_PHONE STRING,
          | S_ACCTBAL DOUBLE,
          | S_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/supplier'
        """.stripMargin),
      sql(
        s"""
          |create external table partsupp (
          | PS_PARTKEY INT,
          | PS_SUPPKEY INT,
          | PS_AVAILQTY INT,
          | PS_SUPPLYCOST DOUBLE,
          | PS_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/partsupp'
        """.stripMargin),
      sql(
        s"""
          |create external table part (
          | P_PARTKEY INT,
          | P_NAME STRING,
          | P_MFGR STRING,
          | P_BRAND STRING,
          | P_TYPE STRING,
          | P_SIZE INT,
          | P_CONTAINER STRING,
          | P_RETAILPRICE DOUBLE,
          | P_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/part'
        """.stripMargin),
      sql(
        s"""
          |create external table nation (
          | N_NATIONKEY INT,
          | N_NAME STRING,
          | N_REGIONKEY INT,
          | N_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/nation'
        """.stripMargin),
      sql(
        s"""
          |create external table region (
          | R_REGIONKEY INT,
          | R_NAME STRING,
          | R_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/region'
        """.stripMargin)
    )

    ddls.foreach(_.collect())
  }

  ignore("drop tables") {
    val tables = Seq("lineitem", "orders", "customer", "supplier",
      "partsupp", "part", "nation", "region")
    val queries = tables.map(table => sql(s"drop table $table"))
    queries.map(_.collect())
  }

  test("show tables") {
    val tables = sql("show tables").collect()
    println("=====================")
    tables.foreach(println(_))
    println("=====================")
  }

  test("table sizes") {
    val tables = Seq("lineitem", "orders", "customer", "supplier", "partsupp",
      "part", "nation", "region")
    val queries = tables.map(table => sql(s"select count(*) from $table"))
    val results = queries.map(_.collect())

    println("====================")
    tables.zip(results).foreach {
      case (table, result) => println(s"# of $table = ${result.mkString}")
    }
    println("====================")
  }
}
