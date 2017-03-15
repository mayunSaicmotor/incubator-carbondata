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

package org.apache.spark.sql.execution

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SortPrefix
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.OrderedDistribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.types.StructType

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines various sort operators.
////////////////////////////////////////////////////////////////////////////////////////////////////



/**
 * Optimized version of [[ExternalSort]] that operates on binary data (implemented as part of
 * Project Tungsten).
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class TungstenMergeSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryNode {

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = false

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val schema = child.schema
    val childOutput = child.output

    /**
     * Set up the sorter in each partition before computing the parent partition.
     * This makes sure our sorter is not starved by other sorters used in the same task.
     */
    def preparePartition(): UnsafeExternalRowSorter = {
      //var start = System.currentTimeMillis()

      val ordering = newOrdering(sortOrder, childOutput)

      // The comparator for comparing prefix
      val boundSortExpression = BindReferences.bindReference(sortOrder.head, childOutput)
      val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

      // The generator for prefix
      val prefixProjection = UnsafeProjection.create(Seq(SortPrefix(boundSortExpression)))
      val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = {
          prefixProjection.apply(row).getLong(0)
        }
      }

      //val pageSize = SparkEnv.get.shuffleMemoryManager.pageSizeBytes
      val sorter = new UnsafeExternalRowSorter(
        schema, ordering, prefixComparator, prefixComputer, 1024)
      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }

      //var end = System.currentTimeMillis()
      //println("preparePartition for sort: " + (end - start))

      sorter
    }

    /** Compute a partition using the sorter already set up previously. */
    def executePartition(
        taskContext: TaskContext,
        partitionIndex: Int,
        sorter: UnsafeExternalRowSorter,
        parentIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
     // var start = System.currentTimeMillis()
      // val sortedIterator = sorter.sort(parentIterator.asInstanceOf[Iterator[UnsafeRow]])
    // taskContext.internalMetricsToAccumulators(
    // InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.getPeakMemoryUsage)

      // var end = System.currentTimeMillis()
     // println("sort query time: " + (end - start))
      parentIterator
    }

    // Note: we need to set up the external sorter in each partition before computing
    // the parent partition, so we cannot simply use `mapPartitions` here (SPARK-9709).
    // new MapPartitionsWithPreparationRDD[InternalRow, InternalRow, UnsafeExternalRowSorter](
      // child.execute(), preparePartition, executePartition, preservesPartitioning = true)

    null
  }

}

object TungstenMergeSort {
  /**
   * Return true if UnsafeExternalSort can sort rows with the given schema, false otherwise.
   */
  def supportsSchema(schema: StructType): Boolean = {
   // UnsafeExternalRowSorter.supportsSchema(schema)
    true
  }

 /*   case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan)
                      (@transient sqlContext: SQLContext) extends UnaryNode {
  override def otherCopyArgs = sqlContext :: Nil

  override def output = child.output

  @transient
  lazy val ordering = new RowOrdering(sortOrder)
  
  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ordering)
  
  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  override def execute() = sqlContext.sparkContext.makeRDD(executeCollect(), 1)
}*/

}
