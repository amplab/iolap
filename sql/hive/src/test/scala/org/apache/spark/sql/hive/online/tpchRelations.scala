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

case class LineItem(
    l_orderkey: Int = 0,
    l_partkey: Int = 0,
    l_suppkey: Int = 0,
    l_linenumber: Int = 0,
    l_quantity: Int = 0,
    l_extendedprice: Double = 0,
    l_discount: Double = 0,
    l_tax: Double = 0,
    l_returnflag: String = "",
    l_shipdate: String = "",
    l_commitdate: String = "",
    l_receiptdate: String = "",
    l_shipinstruct: String = "",
    l_shipmode: String = "",
    l_comment: String = "")

case class Part(
    p_partkey: Int = 0,
    p_name: String = "",
    p_mgfr: String = "",
    p_brand: String = "",
    p_type: String = "",
    p_size: Int = 0,
    p_container: String = "",
    p_retailprice: Double = 0,
    p_comment: String = "")
