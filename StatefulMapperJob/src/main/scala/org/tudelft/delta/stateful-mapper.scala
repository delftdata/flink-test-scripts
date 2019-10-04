package org.tudelft.delta

import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
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
    cons
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
    env.enableCheckpointing(params.getInt("experiment-checkpoint-interval-ms"))// start a checkpoint every 2seconds
    val config = env.getCheckpointConfig
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)// set mode to exactly-once (this is the default)
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val parallelism = params.getInt("experiment-parallelism")

    val sourceStream : DataStream[String] = env.addSource(kafkaSrc(props)).setParallelism(parallelism).disableChaining()
    val keyedSourceStream = sourceStream.keyBy(s => if (s.toLowerCase.length > 0) s.charAt(0) else 'z')

    val depth = params.getInt("experiment-depth")
    var streams : List[DataStream[String]] = List.empty
    streams = streams :+ keyedSourceStream
    for(i <- 1 to depth){
      val newStream =  streams.last.map(new BenchmarkStatefulMapper(props)).setParallelism(parallelism).disableChaining() //Stateful
      //val newStream =  streams.last.map(x => x).setParallelism(parallelism).disableChaining()                               //Stateless
      val keyedNewStream = newStream.keyBy(s => if (s.toLowerCase.length > 0) s.charAt(0) else 'z')
      streams = streams :+ keyedNewStream
    }
    val sink = streams.last.addSink(kafkaSink(props)).setParallelism(parallelism).disableChaining()
    // execute program
    env.execute("Streaming HA Benchmark")
  }
}

class BenchmarkStatefulMapper(properties: Properties) extends RichMapFunction[String, String] {

  var state: ListState[Array[Byte]] = _
  val sleepTime = properties.getProperty("sleep", "1").toLong
  val segmentSize = properties.getProperty("segment-size", "10000").toInt

  val stateSize = properties.getProperty("experiment-state-size").toInt
  val parallelism = properties.getProperty("experiment-parallelism").toInt

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    state = getRuntimeContext.getListState(new ListStateDescriptor[Array[Byte]]("state", createTypeInformation[Array[Byte]]))
  }


  override def map(value: String): String = {
    //Initialize state
    if(! state.get().iterator().hasNext){
      //Share load with other parallel instances
      val stateSizePerOperatorPerKey = (stateSize.toFloat / parallelism / 27).toInt

      // Fill with segments
      for(i <- 1 to (stateSizePerOperatorPerKey / segmentSize))
        state.add(Array.fill(segmentSize)((scala.util.Random.nextInt(256) - 128).toByte))
      // Fill remainder
      state.add(Array.fill(stateSizePerOperatorPerKey % segmentSize)((scala.util.Random.nextInt(256) - 128).toByte))
    }else {
      Thread.sleep(sleepTime)

    }
    value
  }
}
