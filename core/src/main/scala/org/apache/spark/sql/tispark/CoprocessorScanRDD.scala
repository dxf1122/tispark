package org.apache.spark.sql.tispark

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark._
import org.apache.spark.memory.MemoryMode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CoprocessorScanRDD(val dagRequest: TiDAGRequest,
                         val tiConf: TiConfiguration,
                         val tableRef: TiTableReference,
                         val ts: TiTimestamp,
                         val enableBatch: Boolean,
                         @transient private val session: TiSession,
                         @transient private val sparkSession: SparkSession)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {
  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val (dataTypes: List[DataType], rowTransformer: RowTransformer) =
    initializeSchema()

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    dagRequest.resolve()

    def toSparkRow(row: TiRow): Row = {
      val transRow = rowTransformer.transform(row)
      val rowArray = new Array[Any](dataTypes.size)

      for (i <- 0 until transRow.fieldCount) {
        rowArray(i) = transRow.get(i, dataTypes(i))
      }

      Row.fromSeq(rowArray)
    }

    val tiPartition = split.asInstanceOf[TiPartition]
    val session = TiSessionCache.getSession(tiPartition.appId, tiConf)
    val snapshot = session.createSnapshot(ts)
    val coprocessorIterator =
      snapshot.tableRead(dagRequest, tiPartition.tasks)

    val fields = dataTypes.map(
      (dt: DataType) => StructField("", TiUtils.toSparkDataType(dt), nullable = true, null)
    )
    val batchIterator = coprocessorIterator.asScala
      .map(toSparkRow)
      .grouped(4096)
      .map(_.iterator)
      .map(
        (rows: Iterator[Row]) =>
          ColumnVectorUtils.toBatch(
            new org.apache.spark.sql.types.StructType(fields.toArray),
            MemoryMode.ON_HEAP,
            rows
          )
      )

    batchIterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[TiPartition].tasks.head.getHost :: Nil

  override protected def getPartitions: Array[Partition] = {
    val conf = sparkSession.conf
    val keyWithRegionTasks = RangeSplitter
      .newSplitter(session.getRegionManager)
      .splitRangeByRegion(
        dagRequest.getRanges,
        conf.get(TiConfigConst.TABLE_SCAN_SPLIT_FACTOR, "1").toInt
      )

    val taskPerSplit = conf.get(TiConfigConst.TASK_PER_SPLIT, "1").toInt
    val hostTasksMap = new mutable.HashMap[String, mutable.Set[RegionTask]]
      with mutable.MultiMap[String, RegionTask]

    var index = 0
    val result = new ListBuffer[TiPartition]
    for (task <- keyWithRegionTasks) {
      hostTasksMap.addBinding(task.getHost, task)
      val tasks = hostTasksMap(task.getHost)
      if (tasks.size >= taskPerSplit) {
        result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
        index += 1
        hostTasksMap.remove(task.getHost)

      }
    }
    // add rest
    for (tasks <- hostTasksMap.values) {
      result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
      index += 1
    }
    result.toArray
  }
}
