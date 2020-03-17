package org.tudelft.delta

import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011, FlinkKafkaProducer010, FlinkKafkaProducer011}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

object BenchmarkMapper {

  def kafkaSrc(properties: Properties): SourceFunction[String] = {
    val inputTopicName = s"benchmark-input"

    val cons = new FlinkKafkaConsumer011(inputTopicName, new SimpleStringSchema(), properties)
    cons.setStartFromEarliest()
  }

  def kafkaSink(properties: Properties): SinkFunction[String] = {

    val outputTopicName = s"benchmark-output"
    val prod = new FlinkKafkaProducer010[String](outputTopicName, new SimpleStringSchema(), properties)
    //val prod = new FlinkKafkaProducer011[String](outputTopicName, new KeyedSerializationSchema[String] {
    //  override def serializeKey(element: String): Array[Byte] = {return Array.emptyByteArray}

    //  override def serializeValue(element: String): Array[Byte] = {return element.getBytes()}

    //  override def getTargetTopic(element: String): String = {outputTopicName}
    //}, properties, FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE)
    prod
  }

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)
    val props = params.getProperties
    props.setProperty("group.id", "benchmark-group-" + UUID.randomUUID()) //UUID needed so kafka doesnt start from offset
    props.setProperty("retries", "10")

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment.disableOperatorChaining

    env.enableCheckpointing(params.getInt("experiment-checkpoint-interval-ms"))// start a checkpoint every 2seconds
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //val backend = new RocksDBStateBackend("hdfs://hadoop-hadoop-hdfs-nn.default.svc.cluster.local:9000/checkpoints/", false)
    //backend.setDbStoragePath("file:///opt/flink/checkpoints")
    //env.setStateBackend(backend)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    //config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    val parallelism = params.getInt("experiment-parallelism")

    val sourceStream : DataStream[String] = env.addSource(kafkaSrc(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)

    val depth = params.getInt("experiment-depth")
    var streams : List[DataStream[String]] = List.empty
    streams = streams :+ sourceStream.rebalance
    for(i <- 1 to depth){
      val newStream =  streams.last.map(new BenchmarkStatefulMapper(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString) //Stateful
      //val newStream =  streams.last.map(x => x).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID())                               //Stateless
      streams = streams :+  newStream.rebalance
    }
    val sink = streams.last.addSink(kafkaSink(props)).setParallelism(parallelism).disableChaining().slotSharingGroup(UUID.randomUUID().toString)
    // execute program
    env.execute("Streaming HA Benchmark")
  }
}

/*
 * For each key provide the full amount of requested state.
 * Option "per-partition-state" set to false changes this behaviour, dividing by the parallelism
 */
/*
 * For each key provide the full amount of requested state.
 * Option "per-partition-state" set to false changes this behaviour, dividing by the parallelism
 */
class BenchmarkStatefulMapper(properties: Properties) extends MapFunction[String, String] with CheckpointedFunction {

  var state: ListState[Array[Byte]] = _
  val sleepTime: Long = properties.getProperty("sleep", "0").toLong
  val statePerPartition: Boolean = properties.getProperty("per-partition-state", "true").toBoolean
  val stateSize: Int = properties.getProperty("experiment-state-size").toInt
  val parallelism: Int = properties.getProperty("experiment-parallelism").toInt
  val accessState: Boolean = properties.getProperty("experiment-access-state").toBoolean
  val STATE_FRAGMENT_SIZE: Int = properties.getProperty("experiment-state-fragment-size").toInt //0.95 Mebibytes


  val stateAmount: Int = if(statePerPartition) stateSize  else (stateSize / parallelism ).toInt
  val numArraysToWrite: Int = stateAmount / STATE_FRAGMENT_SIZE
  val remainder: Int = stateAmount % STATE_FRAGMENT_SIZE

  override def map(value: String): String = {
    // Avoid issues with possibly the sleep(0) yielding the thread
    if(sleepTime != 0)
      Thread.sleep(sleepTime)
    if(accessState)
      state.get()
    value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {

  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    state = context.getOperatorStateStore.getListState(new ListStateDescriptor[Array[Byte]]("PerOperatorState", createTypeInformation[Array[Byte]]))
    val byteArray: Array[Byte] = Array.fill(STATE_FRAGMENT_SIZE)((scala.util.Random.nextInt(256) - 128).toByte)
    val byteArrayRemainder: Array[Byte] = Array.fill(remainder)((scala.util.Random.nextInt(256) - 128).toByte)
    for (i <- 1 to numArraysToWrite)
      state.add(byteArray) //Force a rewrite of the state, simulating changes of state
    state.add(byteArrayRemainder)
  }
}
