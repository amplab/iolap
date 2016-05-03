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

import java.util.{HashMap => JHashMap, Set => JSet}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Projection, Row}
import org.apache.spark.util.collection.CompactBuffer2

trait MutableIterator[T] extends Iterator[T] {
  def update(value: T): Unit
}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation2.apply]] to create a concrete
 * object.
 */
private[sql] sealed trait HashedRelation2 {
  def get(key: Row): CompactBuffer2[Row]
  def copyOfCertain: HashedRelation2
  def updatableIterator(): MutableIterator[Row]
  def ++=(other: HashedRelation2): HashedRelation2
  def keySet(): JSet[Row]
}

/**
 * A general [[HashedRelation2]] backed by a hash map that maps the key into a sequence of values.
 */
private[joins] case class GeneralHashedRelation2(
    private var hashTable: JHashMap[Row, CompactBuffer2[Row]],
    private var isCertain: Row => Boolean)
  extends HashedRelation2 with Serializable {

  def this() = this(null, null) // Needed for serialization

  override def get(key: Row) = hashTable.get(key)

  override lazy val copyOfCertain = {
    val newHashTable = new JHashMap[Row, CompactBuffer2[Row]]()

    val iterator = hashTable.entrySet().iterator()
    while (iterator.hasNext) {
      val key2Rows = iterator.next()
      val rows = CompactBuffer2[Row]() ++= key2Rows.getValue.filter(isCertain)
      if (rows.nonEmpty) {
        newHashTable.put(key2Rows.getKey, rows)
      }
    }

    new GeneralHashedRelation2(newHashTable, isCertain)
  }

  override def updatableIterator(): MutableIterator[Row] = new MutableIterator[Row] {
    private[this] var pos = 0
    private[this] var rows: CompactBuffer2[Row] = CompactBuffer2[Row]()

    private[this] val iterator = hashTable.entrySet().iterator()

    override def hasNext: Boolean = (pos < rows.size) || (iterator.hasNext && fetchNext())

    override def update(value: Row): Unit = {
      rows.update(pos - 1, value)
    }

    override def next(): Row = {
      val row = rows(pos)
      pos += 1
      row
    }

    private def fetchNext(): Boolean = {
      rows = iterator.next().getValue
      pos = 0
      true
    }
  }

  override def ++=(other: HashedRelation2): HashedRelation2 = {
    // Merge other
    other match {
      case GeneralHashedRelation2(hash, _) =>
        val iterator = hash.entrySet().iterator()
        while (iterator.hasNext) {
          val key2Rows = iterator.next()
          val key = key2Rows.getKey
          val rows = key2Rows.getValue
          var existingRows = hashTable.get(key)
          if (existingRows == null) {
            existingRows = CompactBuffer2[Row]()
            hashTable.put(key, existingRows)
          }
          existingRows ++= rows
          if (rows.hasUncertain) {
            existingRows.hasUncertain = true
          }
        }

      case _ => ???
    }

    this
  }

  override def toString: String = {
    import scala.collection.JavaConversions._
    hashTable.mkString(", ")
  }

  override def keySet(): JSet[Row] = hashTable.keySet()
}

private[joins] case class CombinedHashedRelation(
    private var hashTable1: HashedRelation2,
    private var HashTable2: Broadcast[HashedRelation2],
    private var refresh: () => (HashedRelation2) => HashedRelation2)
  extends HashedRelation2 with Serializable {

  def this() = this(null, null, null) // Needed for serialization

  @transient private[this] lazy val hashTable: HashedRelation2 =
    if (HashTable2 eq null) hashTable1 else refresh()(HashTable2.value.copyOfCertain) ++= hashTable1

  def current = hashTable1

  override def get(key: Row) = hashTable.get(key)

  override def copyOfCertain = hashTable.copyOfCertain

  override def updatableIterator() = hashTable.updatableIterator()

  override def ++=(other: HashedRelation2): HashedRelation2 = hashTable ++= other

  override def toString: String = hashTable.toString

  override def keySet(): JSet[Row] = hashTable.keySet()
}

private[sql] object HashedRelation2 {

  def apply(
      input: Iterator[Row],
      keyGenerator: Projection,
      predicate: (Row => Boolean),
      sizeEstimate: Int = 64): HashedRelation2 = {

    // TODO: Use Spark's HashMap implementation.
    val hashTable = new JHashMap[Row, CompactBuffer2[Row]](sizeEstimate)
    var currentRow: Row = null

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      currentRow = input.next()
      val rowKey = keyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer2[Row]()
          hashTable.put(rowKey, newMatchList)
          newMatchList
        } else {
          existingMatchList
        }
        matchList += currentRow.copy()
        if (!predicate(currentRow)) {
          matchList.hasUncertain = true
        }
      }
    }

    new GeneralHashedRelation2(hashTable, predicate)
  }
}
