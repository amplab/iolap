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

import org.apache.spark._
import org.apache.spark.rdd.{BlockRDD, RDD, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.storage.{BlockId, OLABlockId, StorageLevel}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

class CachedIterator(val iterator: Iterator[Row]) extends Iterator[Row] {
  protected[this] val cache = new ArrayBuffer[Row]()

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Row = {
    val row = iterator.next()
    cache += row.copy()
    row
  }

  def cache(blockId: BlockId): Unit =
    SparkEnv.get.blockManager.putArray(blockId, cache.toArray, StorageLevel.MEMORY_AND_DISK)
}

trait AutoSave {
  self: CachedIterator =>

  val blockId: OLABlockId

  override def hasNext: Boolean = {
    val open = iterator.hasNext
    if (!open) cache(blockId)
    open
  }
}

trait SelectiveCache {
  self: CachedIterator =>

  val predicate: (Row => Boolean)

  override def next(): Row = {
    val row = iterator.next()
    if (predicate(row)) {
      cache += row.copy()
    }
    row
  }
}

class AutoCachedIterator(val blockId: OLABlockId, override val iterator: Iterator[Row])
  extends CachedIterator(iterator) with AutoSave

class SelectiveCachedIterator(val predicate: (Row => Boolean), override val iterator: Iterator[Row])
  extends CachedIterator(iterator) with SelectiveCache

class SelectiveAutoCachedIterator(
    val predicate: (Row => Boolean),
    val blockId: OLABlockId,
    override val iterator: Iterator[Row])
  extends CachedIterator(iterator) with AutoSave with SelectiveCache

class ZippedPartitionsWithIndexRDD2[U: ClassTag, V: ClassTag, T: ClassTag](
    sc: SparkContext,
    var f: (Int, Iterator[U], Iterator[V]) => Iterator[T],
    var rdd1: RDD[U],
    var rdd2: RDD[V])
  extends ZippedPartitionsBaseRDD[T](sc, Seq(rdd1, rdd2)) {

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val partitions = partition.asInstanceOf[ZippedPartitionsPartition].partitions
    f(partition.index, rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

object ComposeRDDFunctions {
  implicit def rddToComposeRDDFunctions[T: ClassTag](rdd: RDD[(T)]): ComposeRDDFunctions[T] =
    new ComposeRDDFunctions(rdd)
}

class ComposeRDDFunctions[U](self: RDD[U])(implicit ut: ClassTag[U]) {
  def zipPartitionsWithIndex[V: ClassTag, T: ClassTag]
    (other: RDD[V])
    (f: (Int, Iterator[U], Iterator[V]) => Iterator[T]): RDD[T] = {
    val sc = self.context
    new ZippedPartitionsWithIndexRDD2(sc, sc.clean(f), self, other)
  }
}

class EmptyPartition(val index: Int) extends Partition

class EmptyIteratorRDD[T: ClassTag](sc: SparkContext, numParts: Int) extends RDD[T](sc, Nil) {
  override protected def getPartitions: Array[Partition] =
    Array.tabulate(numParts)(i => new EmptyPartition(i))

  override def compute(split: Partition, context: TaskContext): Iterator[T] = Iterator()
}

class PartShuffledDependency[T](rdd: RDD[T], @transient mapping: Array[Int])
  extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): Seq[Int] = List(mapping(partitionId))
}

class PartShuffledPartition(val index: Int, val parentSplit: Partition) extends Partition

class PartShuffledRDD[T: ClassTag](sc: SparkContext, @transient rdd: RDD[T], mapping: Array[Int])
  extends RDD[T](sc, new PartShuffledDependency(rdd, mapping) :: Nil) {
  override protected def getPartitions: Array[Partition] =
    Array.tabulate(rdd.partitions.length)(i => new PartShuffledPartition(i, rdd.partitions(i)))

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    firstParent[T].iterator(split.asInstanceOf[PartShuffledPartition].parentSplit, context)
}

object OLABlockRDD {
  def create[T: ClassTag](
    sc: SparkContext,
    opId: Int,
    batchSizes: Array[(Int, Int)],
    reference: RDD[_]): RDD[T] = {

    val blockIds = batchSizes.flatMap { case (size, batchId) =>
      (0 until size).map(OLABlockId(opId, batchId, _).asInstanceOf[BlockId])
    }
    val numParts = reference.partitions.length
    val rdd = new BlockRDD[T](sc, blockIds)
    val padLength = numParts - rdd.partitions.length
    val coalesced =
      if (padLength > 0) {
        rdd.union(new EmptyIteratorRDD(sc, padLength))
      } else {
        rdd
      }.coalesce(numParts)

    // Since numParts is usually small, we use a simple O(n^2) algorithm here
    val locations = reference.partitions.map(reference.preferredLocations)
    val mapping: Array[Int] = new Array[Int](numParts)
    var i = 0
    while (i < numParts) {
      val preferred = coalesced.preferredLocations(coalesced.partitions(i))

      var j = 0
      while (j < numParts && (locations(j) == null || locations(j).intersect(preferred).isEmpty)) {
        j += 1
      }
      if (j < numParts) {
        locations(j) = null
        mapping(i) = j
      } else {
        mapping(i) = -1
      }
      i += 1
    }
    i = 0
    while (i < numParts) {
      if (mapping(i) == -1) {
        var j = 0
        while (j < numParts && locations(j) == null) {
          j += 1
        }
        locations(j) = null
        mapping(i) = j
      }
      i += 1
    }

    new PartShuffledRDD(sc, coalesced, mapping)
  }

  def create[T: ClassTag](
    sc: SparkContext,
    opId: Int,
    batchSizes: Array[(Int, Int)],
    numPartitions: Int): RDD[T] = {

    if (batchSizes.length == 0) {
      new EmptyIteratorRDD[T](sc, numPartitions)
    } else {
      val rdds = batchSizes.map { case (size, batchId) =>
        val blockIds = Array.tabulate(size)(OLABlockId(opId, batchId, _).asInstanceOf[BlockId])
        new BlockRDD[T](sc, blockIds)
      }
      new ZippedPartitionsBaseRDD[T](sc, rdds) {
        override def compute(split: Partition, context: TaskContext): Iterator[T] = {
          val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
          rdds.zip(partitions).toIterator
            .flatMap { case (rdd, part) => rdd.iterator(part, context) }.asInstanceOf[Iterator[T]]
        }
      }
    }
  }
}
