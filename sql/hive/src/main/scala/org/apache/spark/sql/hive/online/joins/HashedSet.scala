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

import org.apache.spark.sql.catalyst.expressions.{MutableProjection, Row}

import scala.collection.JavaConversions._

object HashedSet {
  def apply(iter: Iterator[Row], keyGenerator: MutableProjection): Iterator[Row] = {
    val hashSet = new JHashSet[Row]()
    // Create a Hash set of buildKeys
    while (iter.hasNext) {
      val currentRow = iter.next()
      val rowKey = keyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val keyExists = hashSet.contains(rowKey)
        if (!keyExists) {
          hashSet.add(rowKey.copy())
        }
      }
    }
    hashSet.iterator()
  }

  def apply(iter: Iterator[Row]): JHashSet[Row] = {
    val hashSet = new JHashSet[Row]()
    // Create a Hash set of buildKeys
    while (iter.hasNext) {
      val rowKey = iter.next()
      if (!rowKey.anyNull) {
        val keyExists = hashSet.contains(rowKey)
        if (!keyExists) {
          hashSet.add(rowKey)
        }
      }
    }
    hashSet
  }
}
