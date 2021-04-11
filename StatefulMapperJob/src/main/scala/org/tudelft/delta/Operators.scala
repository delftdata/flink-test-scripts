package org.tudelft.delta

import java.util.{Properties, Random}
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.services.{RandomService, SerializableService, TimeService}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.tudelft.delta.BenchmarkMapper.shuffleKey

import scala.collection.mutable

object Operators {


}

class F extends java.util.function.Function[java.lang.Long, java.lang.Long] {
  override def apply(t: java.lang.Long): java.lang.Long = {
    System.currentTimeMillis() / t
  }
}

class WindowReduceFunction(val numArraysToWrite: Long, val parallelism: Long, val sleepTime: Int) extends ReduceFunction[BenchmarkMessage] {

  @volatile var counter: Int = 0

  override def reduce(x: BenchmarkMessage, y: BenchmarkMessage): BenchmarkMessage = {
    val r = x + y
    r.key = shuffleKey(r.key, numArraysToWrite * parallelism)

    if (sleepTime != 0)
      for (i <- 1 to sleepTime)
        counter += 1
    r
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
class BenchmarkKeyedStatefulMapper(properties: Properties) extends RichMapFunction[BenchmarkMessage, BenchmarkMessage] with CheckpointedFunction {

  val sleepTime: Int = properties.getProperty("sleep", "0").toInt
  val statePerPartition: Boolean = properties.getProperty("per-partition-state", "true").toBoolean
  val stateSize: Int = properties.getProperty("experiment-state-size").toInt
  val parallelism: Int = properties.getProperty("experiment-parallelism").toInt
  val accessState: Float = properties.getProperty("experiment-access-state").toFloat

  val stateAmount: Int = if (statePerPartition) stateSize else stateSize / parallelism
  val numKeysPerPartition: Int = properties.getProperty("num-keys-per-partition", "1000").toInt
  val stateFragmentSize: Int = stateAmount / numKeysPerPartition
  val remainder: Int = stateAmount % stateFragmentSize
  val shuffle: Boolean = properties.getProperty("experiment-shuffle", "true").toBoolean

  val genRandom: Boolean = properties.getProperty("experiment-gen-random", "false").toBoolean
  val genTS: Boolean = properties.getProperty("experiment-gen-ts", "false").toBoolean
  val genSerializable: Boolean = properties.getProperty("experiment-gen-serializable", "false").toBoolean

  @volatile var counter: Int = 0
  var random: Random = new Random(123123)
  var x = 0
  var ts = 0l
  var handle: ValueState[Array[Byte]] = _
  var accessed: ValueState[Boolean] = _

  var randomService: RandomService = _
  var timeService: TimeService = _
  var serializableService: SerializableService[java.lang.Long, java.lang.Long] = _
  //var f: F = _
  override def map(value: BenchmarkMessage): BenchmarkMessage = {
    // Avoid issues with possibly the sleep(0) yielding the thread
    if (sleepTime != 0)
      for (i <- 1 to sleepTime)
        counter += 1


    if (genRandom)
      x = randomService.nextInt()
      //x = random.nextInt()


    if (genTS)
      ts = timeService.currentTimeMillis()
      //ts = System.currentTimeMillis()

    if (genSerializable)
      ts = serializableService.apply(1000)
      //ts = f.apply(1000)

    if (!accessed.value()) {
      accessed.update(true)
      handle.update(randomDataArray(stateFragmentSize))
    }

    if (accessState != 0)
      if (random.nextFloat() < accessState)
        handle.update(randomDataArray(stateFragmentSize))

    if (shuffle)
      value.key = BenchmarkMapper.shuffleKey(value.key, parallelism * numKeysPerPartition)
    value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {
    //this.f = new F()
    this.serializableService = context.getSerializableServiceFactory.build(new F())
    this.randomService = context.getRandomService
    this.timeService = context.getTimeService


    handle = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Array[Byte]]("state", createTypeInformation[Array[Byte]])
    )

    accessed = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Boolean]("accessed", createTypeInformation[Boolean])
    )
  }

  def randomDataArray(size: Int): Array[Byte] = {
    Array.fill(size) {
      (random.nextInt(256) - 128).toByte
    }
  }
}


class BenchmarkStatefulMapper(properties: Properties) extends RichMapFunction[BenchmarkMessage, BenchmarkMessage] with CheckpointedFunction {

  val sleepTime: Int = properties.getProperty("sleep", "0").toInt
  val statePerPartition: Boolean = properties.getProperty("per-partition-state", "true").toBoolean
  val stateSize: Int = properties.getProperty("experiment-state-size").toInt
  val parallelism: Int = properties.getProperty("experiment-parallelism").toInt
  val accessState: Float = properties.getProperty("experiment-access-state").toFloat

  val stateAmount: Int = if (statePerPartition) stateSize else stateSize / parallelism
  val numKeysPerPartition: Int = properties.getProperty("num-keys-per-partition", "1000").toInt
  val stateFragmentSize: Int = stateAmount / numKeysPerPartition
  val remainder: Int = stateAmount % stateFragmentSize
  val shuffle: Boolean = properties.getProperty("experiment-shuffle", "true").toBoolean

  val genRandom: Boolean = properties.getProperty("experiment-gen-random", "false").toBoolean
  val genTS: Boolean = properties.getProperty("experiment-gen-ts", "false").toBoolean
  val genSerializable: Boolean = properties.getProperty("experiment-gen-serializable", "false").toBoolean
  @volatile var counter: Int = 0
  var random: Random = new Random(123123)
  var x = 0
  var ts = 0l

  val state: mutable.Map[Long, ListState[Array[Byte]]] = mutable.Map.empty

  var randomService: RandomService = _
  var timeService: TimeService = _
  var serializableService: SerializableService[java.lang.Long, java.lang.Long] = _
  //var f: F = _

  override def map(value: BenchmarkMessage): BenchmarkMessage = {
    // Avoid issues with possibly the sleep(0) yielding the thread
    if (sleepTime != 0)
      for (i <- 1 to sleepTime)
        counter += 1

    if (genRandom)
      x = randomService.nextInt()
      //x = random.nextInt()


    if (genTS)
      ts = timeService.currentTimeMillis()
      //ts = System.currentTimeMillis()

    if (genSerializable)
      ts = serializableService.apply(1000)
      //f.apply(1000)

    if (accessState != 0)
      if (random.nextFloat() < accessState) {
        val handle = state(value.key % numKeysPerPartition)
        handle.clear()
        handle.add(randomDataArray(stateFragmentSize))
      }

    if (shuffle)
      value.key = BenchmarkMapper.shuffleKey(value.key, parallelism * numKeysPerPartition)
    value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {
    this.serializableService = context.getSerializableServiceFactory.build(new F())
    this.randomService = context.getRandomService
    this.timeService = context.getTimeService
    //this.f = new F()

    for (i <- 0 to numKeysPerPartition) {
      val handle = context.getOperatorStateStore.getListState(new ListStateDescriptor[Array[Byte]]("state-" + i, createTypeInformation[Array[Byte]]))
      handle.add(randomDataArray(stateFragmentSize))
      state.put(i.toLong, handle)
    }
  }

  def randomDataArray(size: Int): Array[Byte] = {
    Array.fill(size) {
      (random.nextInt(256) - 128).toByte
    }
  }
}
