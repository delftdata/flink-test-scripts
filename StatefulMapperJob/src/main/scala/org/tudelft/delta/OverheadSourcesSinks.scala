package org.tudelft.delta

import java.util.Properties

import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}


class ThroughputThrottler(val targetThroughput: Long, val startMs: Long) extends Serializable {

  private val NS_PER_MS: Long = 1000000L
  private val NS_PER_SEC: Long = 1000 * NS_PER_MS
  private val MIN_SLEEP_NS: Long = 2 * NS_PER_MS

  private var sleepTimeNs: Long = if (targetThroughput > 0)  NS_PER_SEC / targetThroughput else Long.MaxValue

  private var sleepDeficitNs: Long = 0
  private var shouldWakeup: Boolean = false


  def shouldThrottle(amountSoFar: Long, sendStartMs: Long): Boolean = {
    if (this.targetThroughput < 0)
      return false
    val elapsedSec = (sendStartMs - startMs) / 1000.0
    elapsedSec > 0 && (amountSoFar / elapsedSec) > targetThroughput
  }

  def throttle(): Unit = {
    if (targetThroughput == 0) {
      try {
        this.synchronized {
          while (!shouldWakeup)
            this.wait()

        }
      }catch {
        case e: InterruptedException => //do nothing
      }
      return
    }

    sleepDeficitNs += sleepTimeNs

    if(sleepDeficitNs >= MIN_SLEEP_NS){
      val sleepStartNs: Long = System.nanoTime()
      try {
        this.synchronized {
          var remaining: Long = sleepDeficitNs
          while (!shouldWakeup && remaining > 0) {
            val sleepMs: Long = remaining / NS_PER_MS
            val sleepNs: Long = remaining - sleepMs * NS_PER_MS
            this.wait(sleepMs, sleepNs.toInt)
            val elapsed: Long = System.nanoTime() - sleepStartNs
            remaining = sleepDeficitNs - elapsed
          }
        }
      } catch {
        case _: InterruptedException =>
          val sleepElapsedNs = System.nanoTime() - sleepStartNs
          if(sleepElapsedNs <= sleepDeficitNs)
            sleepDeficitNs -= sleepElapsedNs
      }
    }
  }

  def wakeup(): Unit = {
    this.synchronized {
      shouldWakeup = true
      this.notifyAll()
    }
  }

}

class OverheadSource(val properties: Properties, val parallelism: Int, val numArraysToWrite: Long) extends RichParallelSourceFunction[BenchmarkMessage] {

  val throttler: ThroughputThrottler = new ThroughputThrottler(properties.getProperty("target-throughput", "-1").toLong / parallelism, System.currentTimeMillis())
  var running = true

  val shuffle: Boolean = properties.getProperty("experiment-shuffle", "true").toBoolean

  override def run(sourceContext: SourceFunction.SourceContext[BenchmarkMessage]): Unit = {
    val keyspace = parallelism * numArraysToWrite
    val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    val benchmarkMessage: BenchmarkMessage = new BenchmarkMessage()

    var counter = 0

    while (running){
      val sendStartMs: Long = System.currentTimeMillis()


      benchmarkMessage.inputTS = sendStartMs
      benchmarkMessage.data = counter * parallelism + subtaskIndex

      if(shuffle)
        benchmarkMessage.key = BenchmarkMapper.shuffleKey(benchmarkMessage.data, keyspace)
      else
        benchmarkMessage.key = benchmarkMessage.data % numArraysToWrite + numArraysToWrite * subtaskIndex

      sourceContext.getCheckpointLock.synchronized {
        sourceContext.collectWithTimestamp(benchmarkMessage, sendStartMs)
      }

      counter+=1

      if(throttler.shouldThrottle(counter,sendStartMs))
        throttler.throttle()
    }

  }

  override def cancel(): Unit = {
    running = false
    throttler.wakeup()
  }

}


object OverheadSourcesSinks {

  def src(properties: Properties, timeChar: String, parallelism: Int, numArraysToWrite: Long): SourceFunction[BenchmarkMessage] = {
    new OverheadSource(properties, parallelism, numArraysToWrite)
  }


  def sink(properties: Properties, transactional: Boolean): SinkFunction[BenchmarkMessage] = {
    new DiscardingSink[BenchmarkMessage]
  }
}
