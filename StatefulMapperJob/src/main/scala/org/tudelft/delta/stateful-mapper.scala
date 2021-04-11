package org.tudelft.delta

import java.util.{Properties, UUID}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


class BenchmarkMessage(var inputTS: Long, var data: Long, var key: Long) extends Serializable {

  def this() = {
    this(0, 0, 0)
  }

  def +(that: BenchmarkMessage): BenchmarkMessage = {
    if (inputTS > that.inputTS) this else that
  }
}

object BenchmarkMapper {

  def shuffleKey(input: Long, keySpace: Long): Long = {
    (input * 103183 + 116803) % keySpace
  }

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)
    val props = params.getProperties
    props.setProperty("group.id", "benchmark-group-" + UUID.randomUUID()) //UUID needed so kafka doesnt start from offset
    props.setProperty("retries", "10")

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment.disableOperatorChaining


    env.enableCheckpointing(params.getInt("experiment-checkpoint-interval-ms"))
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    env.getConfig.setLatencyTrackingInterval(params.getLong("experiment-latency-tracking-interval"))

    val overheadMeasurement = params.getBoolean("experiment-overhead-measurement")
    val parallelism = params.getInt("experiment-parallelism")
    val depth = params.getInt("experiment-depth")
    val windowSizeMs: Long = params.getLong("experiment-window-size")
    val windowSlideMs: Long = params.getLong("experiment-window-slide")

    val timeChar = params.get("experiment-time-char")
    env.setStreamTimeCharacteristic(
      timeChar match {
        case "processing" => TimeCharacteristic.ProcessingTime
        case "ingestion" => TimeCharacteristic.IngestionTime
        case _ => TimeCharacteristic.EventTime
      }
    )
    env.getConfig.setAutoWatermarkInterval(params.getLong("experiment-watermark-interval"))

    //TODO reenable Clonos only params
    env.getConfig.setAutoTimeSetterInterval(params.getLong("experiment-time-setter-interval"))
    env.getJavaEnv.setDeterminantSharingDepth(params.getInt("experiment-determinant-sharing-depth"))

    val shuffle: Boolean = params.getBoolean("experiment-shuffle", true)

    val operatorType = params.get("experiment-operator")
    val transactional = params.getBoolean("experiment-transactional")
    val sleepTime: Int = params.getInt("sleep")
    val numKeysPerPartition: Int = params.getInt("num-keys-per-partition", 1000)

    var sourceStream: DataStream[BenchmarkMessage] = null

    if (overheadMeasurement)
      sourceStream = env.addSource(OverheadSourcesSinks.src(props, timeChar, parallelism, numKeysPerPartition)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    else
      sourceStream = env.addSource(KafkaSourcesSinks.src(props, timeChar, parallelism, numKeysPerPartition)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)

    if (shuffle)
      buildShuffleGraph(sourceStream, parallelism, depth, windowSizeMs, windowSlideMs, operatorType, numKeysPerPartition, sleepTime, overheadMeasurement, transactional, props)
    else
      buildNoShuffleGraph(sourceStream, parallelism, depth, windowSizeMs, windowSlideMs, operatorType, numKeysPerPartition, sleepTime, overheadMeasurement, transactional, props)


    // execute program
    env.execute("Streaming HA Benchmark, Shuffle: " + shuffle)
  }

  def buildNoShuffleGraph(sourceStream: DataStream[BenchmarkMessage], parallelism: Integer, depth: Integer, windowSizeMs: Long, windowSlideMs: Long, operatorType: String, numArraysToWrite: Long, sleepTime: Integer, overheadMeasurement: Boolean, transactional: Boolean, props: Properties): DataStreamSink[BenchmarkMessage] = {
    var streams: List[DataStream[BenchmarkMessage]] = List.empty
    streams = streams :+ sourceStream.forward
    for (i <- 1 to depth) {
      var newStream: DataStream[BenchmarkMessage] = null
      if (operatorType.equals("map") || (operatorType.equals("window") && i != depth))
        newStream = streams.last.map(new BenchmarkStatefulMapper(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
      else if (operatorType.equals("window"))
        newStream = streams.last.timeWindowAll(Time.milliseconds(windowSizeMs), Time.milliseconds(windowSlideMs)).reduce(new WindowReduceFunction(numArraysToWrite, parallelism.toLong, sleepTime)).setParallelism(parallelism.toInt).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
      streams = streams :+ newStream.forward
    }

    if (overheadMeasurement)
      streams.last.addSink(OverheadSourcesSinks.sink(props, transactional)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    else
      streams.last.addSink(KafkaSourcesSinks.sink(props, transactional)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
  }

  def buildShuffleGraph(sourceStream: DataStream[BenchmarkMessage], parallelism: Integer, depth: Integer, windowSizeMs: Long, windowSlideMs: Long, operatorType: String, numArraysToWrite: Long, sleepTime: Integer, overheadMeasurement: Boolean, transactional: Boolean, props: Properties): DataStreamSink[BenchmarkMessage] = {
    var streams: List[KeyedStream[BenchmarkMessage, Long]] = List.empty
    streams = streams :+ sourceStream.keyBy(event => event.key)
    for (i <- 1 to depth) {
      var newStream: DataStream[BenchmarkMessage] = null
      if (operatorType.equals("map") || (operatorType.equals("window") && i != depth))
        newStream = streams.last.map(new BenchmarkKeyedStatefulMapper(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
      else if (operatorType.equals("window"))
        newStream = streams.last.timeWindow(Time.milliseconds(windowSizeMs), Time.milliseconds(windowSlideMs)).reduce(new WindowReduceFunction(numArraysToWrite, parallelism.toLong, sleepTime)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
      streams = streams :+ newStream.keyBy(event => event.key)
    }

    if (overheadMeasurement)
      streams.last.addSink(OverheadSourcesSinks.sink(props, transactional)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    else
      streams.last.addSink(KafkaSourcesSinks.sink(props, transactional)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
  }
}



