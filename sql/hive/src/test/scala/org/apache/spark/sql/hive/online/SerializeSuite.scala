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

import java.io._
import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.serializer._
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SerializeSuite extends FunSuite {
  import org.apache.spark.sql.test.TestSQLContext._

  ignore("kryo readObject and writeObject") {
    class Custom(val a: Int, val b: Int) extends Serializable //with KryoSerializable
    {
      @transient lazy val value = {
        println("intialize value...")
        10
      }

      @throws(classOf[IOException]) // TODO: why touch lazy val?
      private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
        println(s"!!! Java writeObject !!!")
        oos.defaultWriteObject()
      }

      @throws(classOf[IOException])
      private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
        ois.defaultReadObject()
        println(s"!!! Java readObject !!!")
      }

//      override def write(kryo: Kryo, output: Output): Unit = {
//        println(s"!!! Kryo write in KryoSerializable !!!")
//        kryo.writeClassAndObject(output, this) // TODO:
//      }
//
//      override def read(kryo: Kryo, input: Input): Unit = {
//        println(s"!!! Kryo read in KryoSerializable !!!")
//        kryo.readClassAndObject(input) // TODO:
//        value
//        println("!!!")
//      }

      override def toString: String = s"Custom($a, $b, $value)"
    }

    val c1 = new Custom(10, 100)
    val c2 = new Custom(10, 100)

    val javaSerializer = new JavaSerializer(sparkContext.conf).newInstance()
    val kryoSerializer = new KryoSerializer(sparkContext.conf).newInstance()

    println("Java serialization")
    val javaBytes: ByteBuffer = javaSerializer.serialize(c1)

    println("Kryo serialization")
    val kryoBytes: ByteBuffer = kryoSerializer.serialize(c2)

    println("Java deserialization")
    val javaC: Custom = javaSerializer.deserialize(javaBytes)
    println(s"$javaC")

    println("Kryo deserialization")
    val kryoC: Custom = kryoSerializer.deserialize(kryoBytes)
    println(s"$kryoC")
  }

  ignore("case classes under java serializer") {
    class A(val a: Int) extends Serializable
    class B(val b: Int)
    case class C(c: Int) {
      private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
        println(s"!!! Java writeObject !!!")
        oos.defaultWriteObject()
      }
    }

    val javaSerializer = new JavaSerializer(sparkContext.conf).newInstance()

    println("Serializing class extends Serializable...")
    val a = new A(1)
    javaSerializer.serialize(a)
    println("Successs!")

    println("Serializing class without extending Serializable")
    try {
      val b = new B(1)
      javaSerializer.serialize(b)
      println("Successs!")
    } catch {
      case _: NotSerializableException =>
        println("Failure!")
    }

    println("Serializing case class...")
    val c = C(1)
    javaSerializer.serialize(c)
    println("Successs!")
  }

  ignore("case classes under kryo serializer") {
    class A(val a: Int) extends Serializable
    class B(val b: Int)
    case class C(c: Int) extends KryoSerializable {
      override def write(kryo: Kryo, output: Output): Unit = Utils.tryOrIOException {
        println(s"!!! Kryo writeObject !!!")
        kryo.writeClassAndObject(output, this)
      }

      override def read(kryo: Kryo, input: Input): Unit = kryo.readClassAndObject(input)
    }

    val kryoSerializer = new KryoSerializer(sparkContext.conf).newInstance()

    println("Serializing class extends Serializable...")
    val a = new A(1)
    kryoSerializer.serialize(a)
    println("Successs!")

    println("Serializing class without extending Serializable")
    try {
      val b = new B(1)
      kryoSerializer.serialize(b)
      println("Successs!")
    } catch {
      case _: NotSerializableException =>
        println("Failure!")
    }

    println("Serializing case class...")
    val c = C(1)
    kryoSerializer.serialize(c)
    println("Successs!")
  }

  ignore("kryo deep copy") {
    val row = new GenericMutableRow(2)
    val array: ArrayBuffer[Int] = ArrayBuffer(0, 1, 2)
    row(0) = array
    val map = new JHashMap[String, Int]()
    map.put("a", 10)
    map.put("b", 20)
    row(1) = map

    val kryo = new KryoSerializer(sparkContext.conf).newKryo()
//    val rowCopy = new GenericMutableRow(2)
//    rowCopy(0) = kryo.copy(row(0))
//    rowCopy(1) = kryo.copy(row(1))
    val rowCopy = kryo.copy(row)

//    val f: FieldSerializer[ArrayBuffer[Int]] = new FieldSerializer(kryo, array.getClass)
//    println(s"HAN: ${f.copy(kryo, array).eq(array)}")

    println("========================")
    println(s"row = $row")
    println(s"copy of row = $rowCopy")

    println(s"row(0) == rowCopy(0) " +
      s"${row.values(0).asInstanceOf[AnyRef].eq(rowCopy.values(0).asInstanceOf[AnyRef])}")
    println(s"row(1) == rowCopy(1) " +
      s"${row.values(1).asInstanceOf[AnyRef].eq(rowCopy.values(1).asInstanceOf[AnyRef])}")

    array(0) = 3
    map.put("a", 30)

    println(s"row = $row")
    println(s"copy of row = $rowCopy")
  }

  ignore("compactbuffer") {
    val buf = new CompactBuffer[Any]()
    buf += 1
    buf += "String"

    println(s"KAI: ${buf.mkString(", ")}")

    val kryo = new SparkSqlSerializer(sparkContext.conf).newInstance()
    val bytes = kryo.serialize(buf)

    println("serialization done")

    val debuf = kryo.deserialize[CompactBuffer[_]](bytes)

    println("deserialization done")
    println(s"KAI: ${debuf.mkString(", ")}")
  }

  ignore("arrayseq kryo") {
    val buf = new mutable.ArraySeq[Any](2)
    buf(0) = 1
    buf(1) = "String"

    println(s"KAI: ${buf.mkString(", ")}")

    val kryo = new KryoSerializer(sparkContext.conf).newInstance()
    val bytes = kryo.serialize(buf)

    println("serialization done")

    val debuf = kryo.deserialize[mutable.ArraySeq[_]](bytes)

    println("deserialization done")
    println(s"KAI: ${debuf.mkString(", ")}")
  }

  ignore("arrayseq sql") {
    val buf = new mutable.ArraySeq[Any](2)
    buf(0) = 1
    buf(1) = "String"

    println(s"KAI: ${buf.mkString(", ")}")

    val kryo = new SparkSqlSerializer(sparkContext.conf).newInstance()
    val bytes = kryo.serialize(buf)

    println("serialization done")

    val debuf = kryo.deserialize[mutable.ArraySeq[_]](bytes)

    println("deserialization done")
    println(s"KAI: ${debuf.mkString(", ")}")
  }

  ignore("Row sql") {
    val buf = new GenericMutableRow(2)
    buf(0) = 1
    buf(1) = "String"

    println(s"KAI: ${buf.mkString(", ")}")

    val kryo = new SparkSqlSerializer(sparkContext.conf).newInstance()
    val bytes = kryo.serialize(buf)

    println("serialization done")

    val debuf = kryo.deserialize[GenericMutableRow](bytes)

    println("deserialization done")
    println(s"KAI: ${debuf.mkString(", ")}")
  }

  test("build map") {
    val in = new FileInputStream("~/zk1")
    val oin = new ObjectInputStream(in)
    val rows = oin.readObject().asInstanceOf[Array[Row]]
    oin.close()

    println("row sample: " + rows(0).mkString(", "))
    println("row size: " + rows.length)

    println("building...")
    val map = new JHashMap[Row, Row]()
    rows.foreach { row =>
      map.put(row, row)
    }
    println("done!")
  }
}
