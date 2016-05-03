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

import java.util.{HashMap => JHashMap, HashSet => JHashSet, LinkedHashMap => JLinkedHashMap}

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.online.ComposeRDDFunctions._
import org.apache.spark.storage.{LazyBlockId, OLABlockId, StorageLevel}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

trait DelegatedAggregate {
  self: UnaryNode =>

  val partial: Boolean
  val groupingExpressions: Seq[Expression]
  val aggregateExpressions: Seq[NamedExpression]

  override def requiredChildDistribution: Seq[Distribution] =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this delegated aggregate, used for result substitution.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(unbound: Delegate, resultAttribute: AttributeReference)

  private[this] val groupedAggregates = {
    val seen = new JHashSet[Delegate]()
    val grouped = new JLinkedHashMap[AggregateExpression, ArrayBuffer[ComputedAggregate]]()
    aggregateExpressions.foreach { agg =>
      agg.collect {
        case d@Delegate(r, a) if !seen.contains(d) =>
          seen.add(d)
          var buffer = grouped.get(a)
          if (buffer == null) {
            buffer = new ArrayBuffer[ComputedAggregate]()
            grouped.put(a, buffer)
          }
          buffer += ComputedAggregate(
            d, AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
      }
    }

    // Each buffer should be sorted by _.unbound.repetition
    grouped
  }

  /** A list of aggregates that need to be computed for each group. */
  protected[this] val computedAggregates = groupedAggregates.values().flatten.toArray

  private[this] val delegateeMap: JHashMap[ArrayBuffer[Attribute], Delegatee] = {
    def bind(repetitions: Seq[Expression]): Array[Int] = repetitions.map { repetition =>
      BindReferences.bindReference(repetition, child.output) match {
        case BoundReference(ordinal, _, _) => ordinal
        case _ => -1
      }
    }.toArray

    val toDelegatee = new JHashMap[ArrayBuffer[Attribute], Delegatee]()
    groupedAggregates.values().foreach { (as: ArrayBuffer[ComputedAggregate]) =>
      val repetitions = as.map(_.unbound.multiplicity)
      if (!toDelegatee.containsKey(repetitions)) {
        toDelegatee.put(repetitions, Delegatee(bind(repetitions)))
      }
    }
    toDelegatee
  }

  protected[this] val delegatees = delegateeMap.values().toSeq.toArray

  /** A list of aggregates that are actually computed */
  protected[this] val updatedAggregates: Array[DelegateAggregateExpression] = {
    def bind(as: ArrayBuffer[ComputedAggregate]): Delegatee =
      delegateeMap(as.map(_.unbound.multiplicity))

    var offset = 0
    groupedAggregates.map {
      case (Sum(e), as) =>
        val delegate = BindReferences.bindReference(DelegateSum(e, bind(as), offset), child.output)
        offset += as.length
        delegate
      case (Count(e), as) =>
        val delegate = BindReferences.bindReference(DelegateCount(e, bind(as), offset), child.output)
        offset += as.length
        delegate
      case (Count01(e), as) =>
        val delegate = BindReferences.bindReference(DelegateCount01(e, bind(as), offset), child.output)
        offset += as.length
        delegate
    }.toArray
  }

  /** The schema of the result of all aggregate evaluations */
  protected[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  protected[this] def newAggregateBuffer() = {
    val buffer = new Array[DelegateAggregateFunction](delegatees.length + updatedAggregates.length)
    var i = 0
    while (i < delegatees.length) {
      buffer(i) = delegatees(i)
      i += 1
    }
    i = 0
    while (i < updatedAggregates.length) {
      buffer(i + delegatees.length) = updatedAggregates(i).newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  protected[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  protected[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  protected[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }
}

case class BootstrapAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode with DelegatedAggregate {

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new JHashMap[Row, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIterator = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIterator.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIterator.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = delegatees.length
            while (i < currentBuffer.length) {
              currentBuffer(i).evaluate(aggregateResults)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}

trait OnlineAggregate extends IteratorRefresher with Stateful {
  self: UnaryNode =>

  val cacheFilter: Option[Attribute]
  val opId: OpId

  def cache(iter: Iterator[Row]): CachedIterator = cacheFilter match {
    case Some(filter) =>
      new SelectiveCachedIterator(newPredicate(filter, child.output), iter)
    case None => new CachedIterator(iter)
  }
}

case class AggregateWith2Inputs2Outputs(
    cacheFilter: Option[Attribute],
    streamRefresh: RefreshInfo,
    lineageRelayInfo: LineageRelay,
    integrityInfo: Option[IntegrityInfo],
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    val trace: List[Int] = -1 :: Nil,
    val opId: OpId = OpId.newOpId)
  extends UnaryNode with DelegatedAggregate with Relay with OnlineAggregate {

  val partial = false

  private[this] val watcher = controller.getWatcher

  private[this] def tupleQA(index: Int): QA = integrityInfo match {
    case Some(info) =>
      if (groupingExpressions.isEmpty) SingletonQA(info, index) else HashQA(info, index)
    case None => DummyQA
  }

  trait QA {
    def apply(row: Row, group: Row): Row
  }

  object DummyQA extends QA {
    override def apply(row: Row, group: Row): Row = row
  }

  case class SingletonQA(info: IntegrityInfo, index: Int) extends QA {
    private[this] val schema = output ++ info.schema
    private[this] val checkPred = newPredicate(info.check, schema)
    private[this] val updateProjection =
      new InterpretedMutableProjection(info.updateExpressions, schema)
    private[this] val rowWrapper = new SparseMutableRow(info.updateIndexes)
    updateProjection.target(rowWrapper)
    private[this] val joinedRow = new JoinedRow4
    private[this] val defaultRow = new GenericRow(new Array[Any](aggregateExpressions.length))

    private[this] val iter = prevBatches.iterator

    private[this] var prevRow: Row = fetchNext()

    override def apply(row: Row, group: Row): Row = {
      rowWrapper(row)
      do {
        joinedRow(row, prevRow)
      } while (!checkPred(joinedRow) && {
        prevRow = fetchNext()
        true
      })
      updateProjection(joinedRow)
      row
    }

    private[this] def fetchNext(): Row = {
      if (iter.hasNext) {
        val cursor = iter.next()
        if (cursor != prevBatch.get) {
          watcher += cursor
          logError(s"Integrity Error in (Op $opId, Batch $currentBatch) caused by $joinedRow")
        }
        val lazyBlockId = LazyBlockId(opId.id, cursor, index)
        SparkEnv.get.blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY) match {
          case Some(row) => row.asInstanceOf[Row]
          case None => defaultRow
        }
      } else {
        if (prevBatches.nonEmpty) {
          watcher += -1
          logError(s"Integrity Error in (Op $opId, Batch $currentBatch) caused by $joinedRow")
        }
        defaultRow
      }
    }
  }

  case class HashQA(info: IntegrityInfo, index: Int) extends QA {
    private[this] val schema = output ++ info.schema
    private[this] val checkPred = newPredicate(info.check, schema)
    private[this] val updateProjection =
      new InterpretedMutableProjection(info.updateExpressions, schema)
    private[this] val rowWrapper = new SparseMutableRow(info.updateIndexes)
    updateProjection.target(rowWrapper)
    private[this] val joinedRow = new JoinedRow4
    private[this] val defaultRow = new GenericRow(new Array[Any](aggregateExpressions.length))

    private[this] val iter = prevBatches.iterator

    private[this] var previous: JHashMap[Row, Row] = fetchNext()

    override def apply(row: Row, group: Row): Row = {
      rowWrapper(row)
      do {
        var prevRow = previous.get(group)
        if (prevRow == null) {
          prevRow = defaultRow
        }
        joinedRow(row, prevRow)
      } while (!checkPred(joinedRow) && {
        previous = fetchNext()
        true
      })
      updateProjection(joinedRow)
      row
    }

    private[this] def fetchNext(): JHashMap[Row, Row] = {
      if (iter.hasNext) {
        val cursor = iter.next()
        if (cursor != prevBatch.get) {
          watcher += cursor
          logError(s"Integrity Error in (Op $opId, Batch $currentBatch) caused by $joinedRow")
        }
        val lazyBlockId = LazyBlockId(opId.id, cursor, index)
        SparkEnv.get.blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY) match {
          case Some(table) => table.asInstanceOf[JHashMap[Row, Row]]
          case None => new JHashMap[Row, Row]()
        }
      } else {
        if (prevBatches.nonEmpty) {
          watcher += -1
          logError(s"Integrity Error in (Op $opId, Batch $currentBatch) caused by $joinedRow")
        }
        new JHashMap[Row, Row]()
      }
    }
  }

  def retrieveState(rdd: RDD[Row]): RDD[Row] = {
    val batchSizes = prevBatches.map(bId => (controller.olaBlocks(opId, bId), bId)).toArray
    val cached = OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd.partitions.length)
    refresh(cached)
  }

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    val rdd = child.execute()
    controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length
    controller.lazyBlocks((opId, currentBatch)) = rdd.partitions.length
    val state = retrieveState(rdd)

    if (groupingExpressions.isEmpty) {
      rdd.zipPartitionsWithIndex(state) { (index, iter1, iter2) =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null

        // Historical data
        while (iter2.hasNext) {
          currentRow = iter2.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }

        // New data
        val cached = cache(iter1)
        while (cached.hasNext) {
          currentRow = cached.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        cached.cache(OLABlockId(opId.id, currentBatch, index)) // cache

        val prevRow = prevBatch.flatMap { bId =>
          val lazyBlockId = LazyBlockId(opId.id, bId, index)
          SparkEnv.get.blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY)
        }.map(_.asInstanceOf[Row]).orNull

        val aggregateResults = new GenericMutableRow(computedAggregates.length)
        val resultProjection = new InterpretedMutableProjection(resultExpressions, computedSchema)
        val tupleQa = tupleQA(0) // Integrity check

        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }
        val result = tupleQa(resultProjection(aggregateResults), null)

        val filter = buildRelayFilter()
        if (filter(result)) {
          // Broadcast the old results
          val broadcastProjection = buildRelayProjection()
          val toBroadcast = broadcastProjection(result)
          SparkEnv.get.blockManager.putSingle(
            LazyBlockId(opId.id, currentBatch, index), toBroadcast, StorageLevel.MEMORY_AND_DISK)
        }

        // Pass on the new results
        if (prevRow != null) Iterator() else Iterator(result)
      }
    } else {
      rdd.zipPartitionsWithIndex(state) { (index, iter1, iter2) =>
        val hashTable = new JHashMap[Row, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)
        var currentRow: Row = null

        // Historical data
        while (iter2.hasNext) {
          currentRow = iter2.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        // New data
        val cached = cache(iter1)
        while (cached.hasNext) {
          currentRow = cached.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }
        cached.cache(OLABlockId(opId.id, currentBatch, index)) // cache


        val previous = prevBatch.flatMap { bId =>
          val lazyBlockId = LazyBlockId(opId.id, bId, index)
          SparkEnv.get.blockManager.getSingle(lazyBlockId, StorageLevel.MEMORY_ONLY)
        }.map(_.asInstanceOf[JHashMap[Row, Row]]).getOrElse(new JHashMap[Row, Row]())

        val aggregateResults = new GenericMutableRow(computedAggregates.length)
        val joinedRow = new JoinedRow4
        val resultProjection = new InterpretedMutableProjection(resultExpressions,
          computedSchema ++ namedGroups.map(_._2))
        val tupleQa = tupleQA(index) // Integrity check
        val results = new ArrayBuffer[Row](if (prevBatch.isEmpty) hashTable.size() else 16)

        // Broadcast the old results
        val filter = buildRelayFilter()
        val broadcastProjection = buildRelayProjection()
        val toBroadcast = new JHashMap[Row, Row]

        hashTable.foreach { case (currentGroup, currentBuffer) =>
          var i = delegatees.length
          while (i < currentBuffer.length) {
            currentBuffer(i).evaluate(aggregateResults)
            i += 1
          }
          var row = resultProjection(joinedRow(aggregateResults, currentGroup))
          row = tupleQa(row, currentGroup)

          if (filter(row)) {
            toBroadcast.put(currentGroup, broadcastProjection(row))
          }
          if (!previous.contains(currentGroup)) {
            results += row.copy()
          }
        }

        SparkEnv.get.blockManager.putSingle(
          LazyBlockId(opId.id, currentBatch, index), toBroadcast, StorageLevel.MEMORY_AND_DISK)

        // Pass on the new results
        results.iterator
      }
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"

  def newBatch(newTrace: List[Int]) =
    AggregateWith2Inputs2Outputs(cacheFilter, streamRefresh, lineageRelayInfo, integrityInfo,
      groupingExpressions, aggregateExpressions, child)(controller, newTrace, opId)
}

case class AggregateWith2Inputs(
    cacheFilter: Option[Attribute],
    streamRefresh: RefreshInfo,
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    val opId: OpId = OpId.newOpId)
  extends UnaryNode with DelegatedAggregate with OnlineAggregate {

  def retrieveState(rdd: RDD[Row]): RDD[Row] = {
    val batchSizes = prevBatches.map(bId => (controller.olaBlocks(opId, bId), bId)).toArray
    val cached =
      if (partial) OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd)
      else OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd.partitions.length)
    refresh(cached)
  }

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    val rdd = child.execute()
    controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length
    val state = retrieveState(rdd)

    if (groupingExpressions.isEmpty) {
      rdd.zipPartitionsWithIndex(state) { (index, iter1, iter2) =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null

        // Historical data
        while (iter2.hasNext) {
          currentRow = iter2.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }

        // New data
        val cached = cache(iter1)
        while (cached.hasNext) {
          currentRow = cached.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        cached.cache(OLABlockId(opId.id, currentBatch, index)) // cache

        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      rdd.zipPartitionsWithIndex(state) { (index, iter1, iter2) =>
        val hashTable = new JHashMap[Row, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)
        var currentRow: Row = null

        // Historical data
        while (iter2.hasNext) {
          currentRow = iter2.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        // New data
        val cached = cache(iter1)
        while (cached.hasNext) {
          currentRow = cached.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }
        cached.cache(OLABlockId(opId.id, currentBatch, index)) // cache

        new Iterator[Row] {
          private[this] val hashTableIterator = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIterator.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIterator.next()
            val currentGroup: Row = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = delegatees.length
            while (i < currentBuffer.length) {
              currentBuffer(i).evaluate(aggregateResults)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }

  override protected final def otherCopyArgs = controller :: trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"

  def newBatch(newTrace: List[Int]) =
    AggregateWith2Inputs(cacheFilter, streamRefresh,
      partial, groupingExpressions, aggregateExpressions, child)(controller, newTrace, opId)
}
