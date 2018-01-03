/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util
import java.util.concurrent.{Callable, ExecutorCompletionService}

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.iterator.CoprocessIterator
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.util.{KeyRangeUtils, RangeSplitter}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.TiSessionCache
import gnu.trove.list.array
import gnu.trove.list.array.TLongArrayList
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, GenericInternalRow, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.tispark.{CoprocessorScanRDD, TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata}
import org.apache.spark.sql.{Row, SparkSession}
import com.pingcap.tispark.TiDBRelation
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class CoprocessorExec(output: Seq[Attribute],
                           tiRdd: TiRDD,
                           copRdd: CoprocessorScanRDD,
                           enableBatch: Boolean = true)
    extends LeafExecNode
    with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  override val nodeName: String = "CoprocessorExec"
  override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  override val outputOrdering: Seq[SortOrder] = Nil

  lazy val internalRdd: RDD[InternalRow] = RDDConversions.rowToRowRdd(tiRdd, output.map(_.dataType))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    if (enableBatch) {
      copRdd.map { (row: InternalRow) =>
        {
          numOutputRows += 1
          row
        }
      }
    } else {
      internalRdd.mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(index)
        iter.map { r =>
          numOutputRows += 1
          proj(r)
        }
      }
    }
  }

  override def verboseString: String = {
    s"TiDB $nodeName{${tiRdd.dagRequest.toString}}"
  }

  override def simpleString: String = verboseString

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    if (enableBatch) {
      copRdd :: Nil
    } else {
      internalRdd :: Nil
    }

  override protected def doProduce(ctx: CodegenContext): String = {
    if (enableBatch) {
      return doProduceVectorized(ctx)
    }
    val numOutputRows = metricTerm(ctx, "numOutputRows")

    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map {
      case (a, i) =>
        BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = null
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  private def doProduceVectorized(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.freshName("scanTime")
    ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val batch = ctx.freshName("batch")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")

    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val idx = ctx.freshName("batchIdx")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map {
      case (name, i) =>
        ctx.addMutableState(columnVectorClz, name, s"$name = null;")
        s"$name = $batch.column($i);"
    }

    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(
      nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin
    )

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map {
      case (attr, colVar) =>
        genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    s"""
       |if ($batch == null) {
       |  $nextBatch();
       |}
       |while ($batch != null) {
       |  int numRows = $batch.numRows();
       |  while ($idx < numRows) {
       |    int $rowidx = $idx++;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    if (shouldStop()) return;
       |  }
       |  $batch = null;
       |  $nextBatch();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }

  private def genCodeColumnVector(ctx: CodegenContext,
                                  columnVar: String,
                                  ordinal: String,
                                  dataType: DataType,
                                  nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      ctx.freshName("isNull")
    } else {
      "false"
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"${ctx.registerComment(str)}\n" + (if (nullable) {
                                                     s"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${ctx.defaultValue(dataType)} : ($value);
      """
                                                   } else {
                                                     s"$javaType $valueVar = $value;"
                                                   }).trim
    ExprCode(code, isNullVar, valueVar)
  }

}

/**
 * HandleRDDExec is used for scanning handles from TiKV as a LeafExecNode in index plan.
 * Providing handle scan via a TiHandleRDD.
 *
 * @param tiHandleRDD handle source
 */
case class HandleRDDExec(tiHandleRDD: TiHandleRDD) extends LeafExecNode {
  override val nodeName: String = "HandleRDD"

  override lazy val metrics = Map(
    "numOutputRegions" -> SQLMetrics.createMetric(sparkContext, "number of regions")
  )

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  val internalRDD: RDD[InternalRow] =
    RDDConversions.rowToRowRdd(tiHandleRDD, output.map(_.dataType))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRegions = longMetric("numOutputRegions")

    internalRDD.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRegions += 1
        proj(r)
      }
    }
  }

  final lazy val attributeRef = Seq(
    AttributeReference("RegionId", LongType, nullable = false, Metadata.empty)(),
    AttributeReference(
      "Handles",
      ArrayType(LongType, containsNull = false),
      nullable = false,
      Metadata.empty
    )()
  )

  override def output: Seq[Attribute] = attributeRef

  override def verboseString: String = {
    s"TiDB $nodeName{${tiHandleRDD.dagRequest.toString}}"
  }

  override def simpleString: String = verboseString
}

/**
 * RegionTaskExec is used for issuing requests which are generated based on handles retrieved from
 * [[HandleRDDExec]] aggregated by a [[org.apache.spark.sql.execution.aggregate.SortAggregateExec]]
 * with [[org.apache.spark.sql.catalyst.expressions.aggregate.CollectHandles]] as aggregate function.
 *
 * RegionTaskExec will downgrade a index scan plan to table scan plan if handles retrieved from one
 * region exceed spark.tispark.plan.downgrade.index_threshold in your spark config.
 *
 * Refer to code in [[TiDBRelation]] and [[CoprocessorExec]] for further details.
 *
 */
case class RegionTaskExec(child: SparkPlan,
                          output: Seq[Attribute],
                          dagRequest: TiDAGRequest,
                          tiConf: TiConfiguration,
                          ts: TiTimestamp,
                          @transient private val session: TiSession,
                          @transient private val sparkSession: SparkSession)
    extends UnaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numHandles" -> SQLMetrics.createMetric(sparkContext, "number of handles used in double scan"),
    "numDowngradedTasks" -> SQLMetrics.createMetric(sparkContext, "number of downgraded tasks"),
    "numIndexScanTasks" -> SQLMetrics
      .createMetric(sparkContext, "number of index double read tasks")
  )

  private val appId = SparkContext.getOrCreate().appName
  private val downgradeThreshold = session.getConf.getRegionIndexScanDowngradeThreshold

  type TiRow = com.pingcap.tikv.row.Row

  override val nodeName: String = "RegionTaskExec"

  def rowToInternalRow(row: Row, outputTypes: Seq[DataType]): InternalRow = {
    val numColumns = outputTypes.length
    val mutableRow = new GenericInternalRow(numColumns)
    val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
    var i = 0
    while (i < numColumns) {
      mutableRow(i) = converters(i)(row(i))
      i += 1
    }

    mutableRow
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numHandles = longMetric("numHandles")
    val numIndexScanTasks = longMetric("numIndexScanTasks")
    val numDowngradedTasks = longMetric("numDowngradedTasks")
    child
      .execute()
      .mapPartitionsWithIndexInternal { (index, iter) =>
        // For each partition, we do some initialization work
        val logger = Logger.getLogger(getClass.getName)
        logger.info(s"In partition No.$index")
        val session = TiSessionCache.getSession(appId, tiConf)
        val batchSize = session.getConf.getIndexScanBatchSize
        // We need to clear index info in order to perform table scan
        dagRequest.clearIndexInfo()
        val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
        val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
        val finalTypes = rowTransformer.getTypes.toList

        iter.flatMap { row =>
          val handles = row.getArray(1).toLongArray()
          val handleIterator: util.Iterator[Long] = handles.iterator
          var taskCount = 0

          val completionService =
            new ExecutorCompletionService[util.Iterator[TiRow]](session.getThreadPoolForIndexScan)
          var rowIterator: util.Iterator[TiRow] = null

          def feedBatch(): TLongArrayList = {
            val handles = new array.TLongArrayList(512)
            while (handleIterator.hasNext &&
                   handles.size() < batchSize) {
              handles.add(handleIterator.next())
            }
            handles
          }

          def toSparkRow(row: TiRow): Row = {
            val transRow = rowTransformer.transform(row)
            val rowArray = new Array[Any](finalTypes.size)

            for (i <- 0 until transRow.fieldCount) {
              rowArray(i) = transRow.get(i, finalTypes(i))
            }

            Row.fromSeq(rowArray)
          }

          /**
           * Checks whether a request should be downgraded according to some restrictions
           *
           * @return true, the request should be downgraded, false otherwise.
           */
          def shouldDowngrade: Boolean = {
            handles.length > downgradeThreshold
          }

          /**
           * Checks whether the tasks are valid.
           *
           * Currently we only check whether the task list contains only one [[RegionTask]],
           * since in each partition, handles received are from the same region.
           *
           * @param tasks tasks to examine
           */
          def proceedTasksOrThrow(tasks: Seq[RegionTask]): Unit = {
            if (tasks.lengthCompare(1) != 0) {
              throw new RuntimeException(s"Unexpected region task size:${tasks.size}, expecting 1")
            }
          }

          /**
           * If one task's ranges list exceeds some threshold, we split it into tow sub tasks and
           * each has half of the original ranges.
           *
           * @param tasks task list to examine
           * @return split task list
           */
          def splitTasks(tasks: Seq[RegionTask]): mutable.Seq[RegionTask] = {
            val finalTasks = mutable.ListBuffer[RegionTask]()
            tasks.foreach(finalTasks += _)
            while (finalTasks.exists(isTaskRangeSizeInvalid)) {
              val tasksToSplit = mutable.ListBuffer[RegionTask]()
              finalTasks.filter(isTaskRangeSizeInvalid).foreach(tasksToSplit.add)
              tasksToSplit.foreach(task => {
                val newRanges = task.getRanges.grouped(task.getRanges.size() / 2)
                newRanges.foreach(range => {
                  finalTasks += RegionTask.newInstance(task.getRegion, task.getStore, range)
                })
                finalTasks -= task
              })
            }
            logger.info(s"Split ${tasks.size} tasks into ${finalTasks.size} tasks.")
            finalTasks
          }

          def isTaskRangeSizeInvalid(task: RegionTask): Boolean = {
            task == null ||
            task.getRanges.size() > tiConf.getMaxRequestKeyRangeSize
          }

          def submitTasks(tasks: util.List[RegionTask]): Unit = {
            taskCount += 1
            val task = new Callable[util.Iterator[TiRow]] {
              override def call(): util.Iterator[TiRow] = {
                CoprocessIterator.getRowIterator(dagRequest, tasks, session)
              }
            }
            completionService.submit(task)
          }

          if (shouldDowngrade) {
            // Should downgrade to full table scan for a region
            val handleList = new TLongArrayList()
            handles.foreach(handleList.add)
            // Restore original filters to perform table scan logic
            dagRequest.resetFilters(dagRequest.getDowngradeFilters)

            // After `splitHandlesByRegion`, ranges in the task are arranged in order
            var tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitHandlesByRegion(
                dagRequest.getTableInfo.getId,
                handleList
              )
            proceedTasksOrThrow(tasks)

            val taskRanges = tasks.head.getRanges
            logger.warn(
              s"Index scan handle size:${handles.length} exceed downgrade threshold:$downgradeThreshold" +
                s", downgrade to table scan with ${tasks.size()} region tasks, " +
                s"original index scan task has ${taskRanges.size()} ranges, will try to merge."
            )

            // We merge potential separate index ranges from `taskRanges` into one large range
            // and create a new RegionTask
            tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitRangeByRegion(KeyRangeUtils.mergeRanges(taskRanges))
            proceedTasksOrThrow(tasks)

            val task = tasks.head
            logger.info(
              s"Merged ${taskRanges.size()} index ranges to ${task.getRanges.size()} ranges."
            )
            logger.info(
              s"Unary task downgraded, task info:Host={${task.getHost}}, " +
                s"RegionId={${task.getRegion.getId}}, " +
                s"Store={id=${task.getStore.getId},addr=${task.getStore.getAddress}}, " +
                s"Range={${KeyRangeUtils.toString(task.getRanges.head)}}, " +
                s"RangesListSize=${task.getRanges.size()}}"
            )
            numDowngradedTasks += 1

            submitTasks(tasks)
          } else {
            // Request doesn't need to be downgraded
            while (handleIterator.hasNext) {
              val handleList = feedBatch()
              numHandles += handleList.size()
              logger.info("Single batch handles size:" + handleList.size())
              numIndexScanTasks += 1
              var tasks = RangeSplitter
                .newSplitter(session.getRegionManager)
                .splitHandlesByRegion(
                  dagRequest.getTableInfo.getId,
                  handleList
                )
              proceedTasksOrThrow(tasks)
              tasks = splitTasks(tasks)

              logger.info(s"Single batch RegionTask size:${tasks.size()}")
              tasks.foreach(task => {
                logger.info(
                  s"Single batch RegionTask={Host:${task.getHost}," +
                    s"Region:${task.getRegion}," +
                    s"Store:{id=${task.getStore.getId},address=${task.getStore.getAddress}}, " +
                    s"RangesListSize:${task.getRanges.size()}}"
                )
              })

              submitTasks(tasks)
            }
          }

          // The result iterator serves as an wrapper to the final result we fetched from region tasks
          val resultIter = new util.Iterator[UnsafeRow] {
            override def hasNext: Boolean = {

              def proceedNextBatchTask(): Boolean = {
                // For each batch fetch job, we get the first rowIterator with row data
                while (taskCount > 0) {
                  rowIterator = completionService.take().get()
                  taskCount -= 1

                  // If current rowIterator has any data, return true
                  if (rowIterator.hasNext) {
                    return true
                  }
                }
                // No rowIterator in any remaining batch fetch jobs contains data, return false
                false
              }

              // RowIterator has not been initialized
              if (rowIterator == null) {
                proceedNextBatchTask()
              } else {
                if (rowIterator.hasNext) {
                  return true
                }
                proceedNextBatchTask()
              }
            }

            override def next(): UnsafeRow = {
              numOutputRows += 1
              // Unsafe row projection
              val proj = UnsafeProjection.create(schema)
              proj.initialize(index)
              val sparkRow = toSparkRow(rowIterator.next())
              val outputTypes = output.map(_.dataType)
              // Need to convert spark row to internal row for Catalyst
              proj(rowToInternalRow(sparkRow, outputTypes))
            }
          }
          resultIter
        }
      }
  }

  override def verboseString: String = {
    s"TiSpark $nodeName{downgradeThreshold=$downgradeThreshold,downgradeFilter=${dagRequest.getFilter}"
  }

  override def simpleString: String = verboseString
}
