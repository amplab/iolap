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

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._

class OnlineSQLSuite extends FunSuite {
  val url = ""
  val master = s"spark://$url:7077"

  val sparkContext = new SparkContext(master, "TestSQLContext",
    new SparkConf().set("spark.sql.test", ""))
  val sqlContext = new HiveContext(sparkContext)

  import sqlContext._

  def printResult(query: DataFrame, batchId: Int): Unit = {
    query.queryExecution.executedPlan // force to initialize the query plan

    var rows: Array[Row] = null
    benchmark {
      rows = query.collect()
    }

    println(s"============{batch = $batchId}=============")
    rows.foreach(row => println(row.mkString(", ")))
    println("====================================")
  }

  setConf(STREAMED_RELATIONS, "lineitem")
  setConf(NUMBER_BATCHES, "6")

  ignore("Q1") {
    val query = sql(
      """
        |SELECT l_returnflag
        |	,l_linestatus
        |	,sum(l_quantity) AS sum_qty
        |	,sum(l_extendedprice) AS sum_base_price
        |	,sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price
        |	,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge
        |	,avg(l_quantity) AS avg_qty
        |	,avg(l_extendedprice) AS avg_price
        |	,avg(l_discount) AS avg_disc
        |	,count(*) AS count_order
        |FROM lineitem
        |WHERE l_shipdate <= '1998-09-01'
        |GROUP BY l_returnflag
        |	,l_linestatus
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q3") {
    val query = sql(
      """
        |SELECT o_orderdate
        |	,o_shippriority
        |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM customer
        |	,orders
        |	,lineitem
        |WHERE c_mktsegment = 'BUILDING'
        |	AND c_custkey = o_custkey
        |	AND l_orderkey = o_orderkey
        |	AND o_orderdate < '1995-07-01'
        |	AND o_orderdate > '1994-01-01'
        |	AND l_shipdate > '1994-01-01'
        |GROUP BY o_orderdate
        |	,o_shippriority
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q5") {
    val query = sql(
      """
        |SELECT n_name
        |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM customer
        |	,orders
        |	,lineitem
        |	,supplier
        |	,nation
        |	,region
        |WHERE c_custkey = o_custkey
        |	AND l_orderkey = o_orderkey
        |	AND l_suppkey = s_suppkey
        |	AND c_nationkey = s_nationkey
        |	AND s_nationkey = n_nationkey
        |	AND n_regionkey = r_regionkey
        |	AND r_name = 'AMERICA'
        |	AND o_orderdate >= '1995-01-01'
        |	AND o_orderdate < '1996-01-01'
        |GROUP BY n_name
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q6") {
    val query = sql(
      """
        |SELECT sum(l_extendedprice * l_discount) AS revenues
        |FROM lineitem
        |WHERE l_shipdate >= '1996-01-01'
        |	AND l_shipdate < '1997-01-01'
        |	AND l_discount BETWEEN 0.06 AND 0.08
        |	AND l_quantity < 24
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q7") {
    val query = sql(
      """
        |SELECT supp_nation
        |	,cust_nation
        |	,l_year
        |	,sum(volume) AS revenue
        |FROM (
        |	SELECT n1.n_name AS supp_nation
        |		,n2.n_name AS cust_nation
        |		,substring(l_shipdate, 1, 4) AS l_year
        |		,l_extendedprice * (1 - l_discount) AS volume
        |	FROM supplier
        |		,lineitem
        |		,orders
        |		,customer
        |		,nation n1
        |		,nation n2
        |	WHERE s_suppkey = l_suppkey
        |		AND o_orderkey = l_orderkey
        |		AND c_custkey = o_custkey
        |		AND s_nationkey = n1.n_nationkey
        |		AND c_nationkey = n2.n_nationkey
        |		AND (
        |			(
        |				n1.n_name = 'VIETNAM'
        |				AND n2.n_name = 'KENYA'
        |				)
        |			OR (
        |				n1.n_name = 'KENYA'
        |				AND n2.n_name = 'VIETNAM'
        |				)
        |			)
        |		AND l_shipdate BETWEEN '1995-01-01'
        |			AND '1996-12-31'
        |	) AS shipping
        |GROUP BY supp_nation
        |	,cust_nation
        |	,l_year
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q8") {
    val query = sql(
      """
        |SELECT o_year
        |	,sum(CASE
        |			WHEN nation = 'JAPAN'
        |				THEN volume
        |			ELSE 0
        |			END) / sum(volume) AS mkt_share
        |FROM (
        |	SELECT substring(o_orderdate, 1, 4) AS o_year
        |		,l_extendedprice * (1 - l_discount) AS volume
        |		,n2.n_name AS nation
        |	FROM lineitem
        |   ,part
        |		,supplier
        |		,orders
        |		,customer
        |		,nation n1
        |		,nation n2
        |		,region
        |	WHERE p_partkey = l_partkey
        |		AND s_suppkey = l_suppkey
        |		AND l_orderkey = o_orderkey
        |		AND o_custkey = c_custkey
        |		AND c_nationkey = n1.n_nationkey
        |		AND n1.n_regionkey = r_regionkey
        |		AND r_name = 'ASIA'
        |		AND s_nationkey = n2.n_nationkey
        |		AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
        |		AND p_type = 'LARGE POLISHED BRASS'
        |	) AS all_nations
        |GROUP BY o_year
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q9") {
    val query = sql(
      """
        |SELECT nation
        |	,o_year
        |	,sum(amount) AS sum_profit
        |FROM (
        |	SELECT n_name AS nation
        |		,substring(o_orderdate, 1, 4) AS o_year
        |		,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        |	FROM lineitem
        |   ,part
        |		,supplier
        |		,partsupp
        |		,orders
        |		,nation
        |	WHERE s_suppkey = l_suppkey
        |		AND ps_suppkey = l_suppkey
        |		AND ps_partkey = l_partkey
        |		AND p_partkey = l_partkey
        |		AND o_orderkey = l_orderkey
        |		AND s_nationkey = n_nationkey
        |		AND p_name LIKE '%ghost%'
        |	) AS profit
        |GROUP BY nation
        |	,o_year
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q10") {
    val query = sql(
      """
        |SELECT n_name
        |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM customer
        |	,orders
        |	,lineitem
        |	,nation
        |WHERE c_custkey = o_custkey
        |	AND l_orderkey = o_orderkey
        |	AND o_orderdate >= '1994-10-01'
        |	AND o_orderdate < '1995-01-01'
        |	AND l_returnflag = 'R'
        |	AND c_nationkey = n_nationkey
        |GROUP BY n_name
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q11") {
    setConf(STREAMED_RELATIONS, "partsupp")
    setConf(NUMBER_BATCHES, "4")

    val query = sql(
      """
        |SELECT n_nationkey
        |	,value
        |FROM (
        |	SELECT 0 AS KEY
        |		,n_nationkey
        |		,sum(ps_supplycost * ps_availqty) AS value
        |	FROM partsupp
        |		,supplier
        |		,nation
        |	WHERE ps_suppkey = s_suppkey
        |		AND s_nationkey = n_nationkey
        |	GROUP BY n_nationkey
        |	) AS A
        |	,(
        |		SELECT 0 AS KEY
        |			,sum(ps_supplycost * ps_availqty) * 0.00002 AS threshold
        |		FROM partsupp
        |			,supplier
        |			,nation
        |		WHERE ps_suppkey = s_suppkey
        |			AND s_nationkey = n_nationkey
        |		) AS B
        |WHERE A.KEY = B.KEY
        |	AND value > threshold
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q12") {
    val query = sql(
      """
        |SELECT l_shipmode
        |	,sum(CASE
        |			WHEN o_orderpriority = '1-URGENT'
        |				OR o_orderpriority = '2-HIGH'
        |				THEN 1
        |			ELSE 0
        |			END) AS high_line_count
        |	,sum(CASE
        |			WHEN o_orderpriority <> '1-URGENT'
        |				AND o_orderpriority <> '2-HIGH'
        |				THEN 1
        |			ELSE 0
        |			END) AS low_line_count
        |FROM orders
        |	,lineitem
        |WHERE o_orderkey = l_orderkey
        |	AND l_shipmode IN (
        |		'RAIL'
        |		,'MAIL'
        |		)
        |	AND l_commitdate < l_receiptdate
        |	AND l_shipdate < l_commitdate
        |	AND l_receiptdate >= '1993-01-01'
        |	AND l_receiptdate < '1994-01-01'
        |GROUP BY l_shipmode
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q14") {
    val query = sql(
      """
        |SELECT 100.00 * sum(CASE
        |			WHEN p_type LIKE 'PROMO%'
        |				THEN l_extendedprice * (1 - l_discount)
        |			ELSE 0
        |			END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
        |FROM lineitem
        |	,part
        |WHERE l_partkey = p_partkey
        |	AND l_shipdate >= '1996-12-01'
        |	AND l_shipdate < '1997-01-01'
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q16") {
    setConf(STREAMED_RELATIONS, "partsupp")
    setConf(NUMBER_BATCHES, "4")

    val query = sql(
      """
        |SELECT p_type
        |	,p_size
        |	,count(ps_suppkey) AS supplier_cnt
        |FROM (
        |	SELECT p_brand
        |		,p_type
        |		,p_size
        |		,ps_suppkey
        |	FROM partsupp
        |		,part
        |	WHERE p_partkey = ps_partkey
        |		AND p_brand <> 'Brand#43'
        |		AND p_type NOT LIKE 'STANDARD BURNISHED%'
        |		AND p_size IN (
        |			12
        |			,16
        |			,19
        |			,14
        |			,9
        |			,11
        |			,42
        |			,30
        |			)
        |	) A
        |JOIN (
        |	SELECT s_suppkey
        |	FROM supplier
        |	WHERE s_comment NOT LIKE '%Customer%Complaints%'
        | GROUP BY s_suppkey
        |	) B ON ps_suppkey = s_suppkey
        |GROUP BY p_brand
        |	,p_type
        |	,p_size
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q17") {
    val query = sql(
      """
        |SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
        |FROM (
        |	SELECT p_mfgr
        |		,l_quantity
        |		,l_extendedprice
        |	FROM lineitem
        |		,part
        |	WHERE p_partkey = l_partkey
        |		AND p_brand = 'Brand#13'
        |		AND p_container = 'JUMBO BOX'
        |	) AS A
        |	,(
        |		SELECT p_mfgr
        |			,12 * avg(l_quantity) AS threshold
        |		FROM lineitem
        |			,part
        |		WHERE l_partkey = p_partkey
        |		GROUP BY p_mfgr
        |		) AS B
        |WHERE A.p_mfgr = B.p_mfgr
        |	AND l_quantity < threshold
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q18") {
    val query = sql(
      """
        |SELECT c_nationkey
        |	,sum(l_quantity)
        |FROM (
        |	SELECT c_nationkey
        |		,l_quantity
        |		,o_orderpriority
        |	FROM customer
        |		,orders
        |		,lineitem
        |	WHERE c_custkey = o_custkey
        |		AND o_orderkey = l_orderkey
        |	) A
        |JOIN (
        |SELECT o_orderpriority
        |FROM (
        |	SELECT o_orderpriority, sum(l_quantity) AS tot_qty
        |	FROM orders
        |		,lineitem
        |	WHERE o_orderkey = l_orderkey
        |	GROUP BY o_orderpriority
        | ) B
        |WHERE tot_qty > 31000
        |	) C ON (A.o_orderpriority = C.o_orderpriority)
        |GROUP BY c_nationkey
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q19") {
    val query = sql(
      """
        |SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
        |FROM lineitem
        |	,part
        |WHERE p_partkey = l_partkey
        |	AND l_shipmode IN (
        |		'RAIL'
        |		,'AIR REG'
        |		)
        |	AND l_shipinstruct = 'DELIVER IN PERSON'
        |	AND (
        |		(
        |			p_brand = 'Brand#45'
        |			AND p_container IN (
        |				'SM CASE'
        |				,'SM BOX'
        |				,'SM PACK'
        |				,'SM PKG'
        |				)
        |			AND l_quantity >= 7
        |			AND l_quantity <= 7 + 10
        |			AND p_size BETWEEN 1 AND 50
        |			)
        |		OR (
        |			p_brand = 'Brand#51'
        |			AND p_container IN (
        |				'MED BAG'
        |				,'MED BOX'
        |				,'MED PKG'
        |				,'MED PACK'
        |				)
        |			AND l_quantity >= 20
        |			AND l_quantity <= 20 + 10
        |			AND p_size BETWEEN 1 AND 10
        |			)
        |		OR (
        |			p_brand = 'Brand#51'
        |			AND p_container IN (
        |				'LG CASE'
        |				,'LG BOX'
        |				,'LG PACK'
        |				,'LG PKG'
        |				)
        |			AND l_quantity >= 28
        |			AND l_quantity <= 28 + 10
        |			AND p_size BETWEEN 1 AND 15
        |			)
        |		)
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q20") {
    val query = sql(
      """
        |SELECT s_name
        |	,s_address
        |FROM (
        |	SELECT s_name
        |		,s_address
        |		,s_suppkey
        |	FROM supplier
        |		,nation
        |	WHERE s_nationkey = n_nationkey
        |		AND n_name = 'ETHIOPIA'
        |	) A
        |JOIN (
        |	SELECT ps_suppkey
        |	FROM partsupp
        |	JOIN (
        |		SELECT p_partkey
        |		FROM part
        |		WHERE p_name LIKE 'cornsilk%'
        |   GROUP BY p_partkey
        |		) C ON (ps_partkey = p_partkey)
        |	JOIN (
        |		SELECT s_suppkey
        |			,qty
        |		FROM (
        |			SELECT s_nationkey AS nationkey
        |				,sum(l_quantity) * 0.0001 AS qty
        |			FROM lineitem
        |				,supplier
        |			WHERE l_suppkey = s_suppkey
        |				AND l_shipdate >= '1996-01-01'
        |				AND l_shipdate < '1997-01-01'
        |			GROUP BY s_nationkey
        |			) lqty
        |			,supplier
        |		WHERE s_nationkey = nationkey
        |		) D ON (ps_suppkey = s_suppkey)
        |	WHERE ps_availqty > qty
        | GROUP BY ps_suppkey
        |	) B ON (s_suppkey = ps_suppkey)
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }

  ignore("Q22") {
    setConf(STREAMED_RELATIONS, "customer")
    setConf(NUMBER_BATCHES, "3")

    val query = sql(
      """
        |SELECT cntrycode
        |	,count(*) AS numcust
        |	,sum(c_acctbal) AS totacctbal
        |FROM (
        |	SELECT 0 AS KEY
        |		,substring(c_phone, 1, 2) AS cntrycode
        |		,c_acctbal
        |	FROM customer
        |	WHERE substring(c_phone, 1, 2) IN (
        |			'25'
        |			,'23'
        |			,'11'
        |			,'18'
        |			,'32'
        |			,'33'
        |			,'27'
        |			)
        |	) AS A
        |	,(
        |		SELECT 0 AS KEY
        |			,avg(c_acctbal) AS threshold
        |		FROM customer
        |		WHERE c_acctbal > 0.00
        |			AND substring(c_phone, 1, 2) IN (
        |				'25'
        |				,'23'
        |				,'11'
        |				,'18'
        |				,'32'
        |				,'33'
        |				,'27'
        |				)
        |		) AS B
        |WHERE A.KEY = B.KEY
        |	AND c_acctbal > threshold
        |GROUP BY cntrycode
      """.stripMargin).online

    var batchId = 0
    while (query.hasNext) {
      printResult(query.next(), batchId)
      batchId += 1
    }
  }
}
