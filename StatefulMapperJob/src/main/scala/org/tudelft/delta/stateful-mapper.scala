package org.tudelft.delta

import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object BenchmarkMapper {

  def kafkaSrc(properties: Properties): SourceFunction[String] = {
    val inputTopicName = s"benchmark-input"

    val cons = new FlinkKafkaConsumer011(inputTopicName, new SimpleStringSchema(), properties)
    cons.setStartFromEarliest()
  }

  def kafkaSink(properties: Properties): SinkFunction[String] = {

    val outputTopicName = s"benchmark-output"

    new FlinkKafkaProducer011(outputTopicName, new SimpleStringSchema(), properties)
  }

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)
    val props = params.getProperties
    props.setProperty("group.id", "benchmark-group-" + UUID.randomUUID()) //UUID needed so kafka doesnt start from offset

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment.disableOperatorChaining

    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    //env.setStateBackend(new RocksDBStateBackend(stateDir, false))
    //env.enableCheckpointing(params.getInt("experiment-checkpoint-interval-ms"))// start a checkpoint every 2seconds
    //val config = env.getCheckpointConfig
    //config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)// set mode to exactly-once (this is the default)
    //config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val parallelism = params.getInt("experiment-parallelism")

    val sourceStream : DataStream[String] = env.addSource(kafkaSrc(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    val keyedSourceStream = sourceStream.keyBy(s => if (s.toLowerCase.length > 0) s.charAt(0) else 'z')

    val depth = params.getInt("experiment-depth")
    var streams : List[DataStream[String]] = List.empty
    streams = streams :+ keyedSourceStream
    for(i <- 1 to depth){
      val newStream =  streams.last.map(new BenchmarkStatefulMapper(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString) //Stateful
      //val newStream =  streams.last.map(x => x).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID())                               //Stateless
      val keyedNewStream = newStream.keyBy(s => if (s.toLowerCase.length > 0) s.charAt(0) else 'z')
      streams = streams :+ keyedNewStream
    }
    val sink = streams.last.addSink(kafkaSink(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    // execute program
    env.execute("Streaming HA Benchmark")
  }
}

/*
 * For each key (a-z) provide the full amount of requested state.
 * Option "per-partition-state" set to false changes this behaviour, dividing by the parallelism
 */
class BenchmarkStatefulMapper(properties: Properties) extends RichMapFunction[String, String] {

  var state: ValueState[Array[Byte]] = _
  val sleepTime = properties.getProperty("sleep", "1").toLong
  
  val statePerPartition = properties.getProperty("per-partition-state", "true").toBoolean

  val stateSize = properties.getProperty("experiment-state-size").toInt
  val parallelism = properties.getProperty("experiment-parallelism").toInt
  val stateSizePerSubtaskPerKey = (stateSize.toFloat / parallelism / 27).toInt
  val stateSizePerKey = (stateSize.toFloat / 27).toInt

  val stateAmount = if(statePerPartition) stateSizePerKey else stateSizePerSubtaskPerKey

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    state = getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("state", createTypeInformation[Array[Byte]]))
  }


  override def map(value: String): String = {
    //Initialize state
    val currState = state.value()
    if(currState == null){
      //Share load with other parallel instance
      state.update(Array.fill(stateAmount)((scala.util.Random.nextInt(256) - 128).toByte))
    }else {
      if(sleepTime != 0)
        Thread.sleep(sleepTime)
    }
    value
  }
}
