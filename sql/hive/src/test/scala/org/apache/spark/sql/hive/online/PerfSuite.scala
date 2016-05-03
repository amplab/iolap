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

import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, JoinedRow2, GenericRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types.{ByteType, DoubleType, LongType}
import org.scalatest.FunSuite

import scala.annotation.switch

class PerfSuite extends FunSuite {

  test("joined row vs copy") { // copy is better than joined row up to m = 200
    val n =  50000000
    val m = 200

    benchmark {
      var i = 0
      val row1 = new GenericMutableRow(m)
      val row2 = new GenericMutableRow(0)
      val joinedRow = new JoinedRow2()
      while (i < n) {
        joinedRow(row1, row2)
        var j = 0
        while (j < m) {
          joinedRow(j)
          j += 1
        }
        i += 1
      }
    }

    benchmark {
      var i = 0
      val row1 = new GenericRow(m)
      val row2 = new GenericMutableRow(m)
      while (i < n) {
        var j = 0
        while (j < m) {
          row2(j) = row1(j)
          j += 1
        }
        j = 0
        while (j < m) {
          row2(j)
          j += 1
        }
        i += 1
      }
    }
  }

  ignore("randomness") {
    val n = 5000000

    var sum = 0

    var i = 0
    var current = 5
    while (i < n) {
      current ^= (current << 13)
      current ^= (current >> 17)
      current ^= (current << 5)
      if ((current & 0xFFFFFFFFL) * 2.3283064365386963E-10D < 0.5) {
        sum += 1
      }
      i += 1
    }

    println(s"${sum * 1.0 / n}")

    println((1.0 / 2.3283064365386963E-10D).toLong)
  }

  ignore("random") {
    val n = 50000000

    benchmark {
      var i = 0
      var current = 10
      while (i < n) {
        current ^= (current << 13)
        current ^= (current >> 17)
        current ^= (current << 5)
        i += 1
      }
    }

    benchmark {
      var current = 10
      var i = 0
      while (i < n) {
        current *= 663608941
        i += 1
      }
    }
  }

  ignore("func call") {
    def getFunc1: Any => Any = {
      val numeric = ByteType.numeric.asInstanceOf[Numeric[Any]]
      b => numeric.toLong(b)
    }

    def getFunc2: Any => Any = {
      val numeric = ByteType.numeric.asInstanceOf[Numeric[Any]]
      numeric.toLong
    }

    val func1 = getFunc1
    val func2 = getFunc2

    val n = 5000000

    println("func1...")
    benchmark {
      var i = 0
      while (i < n) {
        func1(i.toByte)
        i += 1
      }
    }

    println("func2...")
    benchmark {
      var i = 0
      while (i < n) {
        func2(i.toByte)
        i += 1
      }
    }
  }

  ignore("scaling") {
    val byteNumeric = ByteType.numeric.asInstanceOf[Numeric[Any]]
    val doubleNumeric = DoubleType.numeric.asInstanceOf[Numeric[Any]]

    val n = 5000000

    println("func1...")
    benchmark {
      var i = 0
      val k = 1
      while (i < n) {
        val j = doubleNumeric.times(i.toDouble, byteNumeric.toDouble(k.toByte))
        i += 1
      }
    }

    println("func2...")
    benchmark {
      var i = 0
      val k = 1
      while (i < n) {
        val j = (k: @switch) match {
          case 0 => 0D
          case 1 => i.toDouble
          case _ => doubleNumeric.times(i.toDouble, byteNumeric.toDouble(k.toByte))
        }
        i += 1
      }
    }
  }

  ignore("lazy val") {
    val numeric = DoubleType.numeric.asInstanceOf[Numeric[Any]]
    lazy val lazyNumeric = DoubleType.numeric.asInstanceOf[Numeric[Any]]

    val n = 10000000

    println("func1...")
    benchmark {
      var i = 0
      while (i < n) {
        val j = numeric.times(i.toDouble, i.toDouble)
        i += 1
      }
    }

    println("func2...")
    benchmark {
      var i = 0
      while (i < n) {
        val j = lazyNumeric.times(i.toDouble, i.toDouble)
        i += 1
      }
    }
  }

  ignore("compare") {
    val numeric = ByteType.numeric.asInstanceOf[Numeric[Any]]

    val n = 10000000

    println("func1...")
    benchmark {
      var i = 0
      while (i < n) {
        val j: Any = i.toByte
        numeric.compare(j, 0.toByte) > 0
        i += 1
      }
    }

    println("func2...")
    benchmark {
      var i = 0
      while (i < n) {
        val j: Any = i.toByte
        j.asInstanceOf[Byte] > 0.toByte
        i += 1
      }
    }
  }

  ignore("sum") {
    val byteNumeric = ByteType.numeric.asInstanceOf[Numeric[Any]]
    val longNumeric = LongType.numeric.asInstanceOf[Numeric[Any]]

    val n = 10000000

    var sumBoxed: Any = 0.toLong

    println("func1...")
    benchmark {
      var i = 0
      while (i < n) {
        val j: Any = i.toByte
        sumBoxed = longNumeric.plus(sumBoxed, byteNumeric.toLong(j))
        i += 1
      }
    }

    var sumUnboxed = 0.toLong

    println("func2...")
    benchmark {
      var i = 0
      while (i < n) {
        val j: Any = i.toByte
        sumUnboxed += j.asInstanceOf[Byte]
        i += 1
      }
    }
  }
}
