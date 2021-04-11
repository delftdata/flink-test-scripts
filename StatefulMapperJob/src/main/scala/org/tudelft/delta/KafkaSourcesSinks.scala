package org.tudelft.delta

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util.Properties
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchemaWrapper, SerializationSchema}
import org.apache.flink.streaming.api.scala._



private class MyExtractor extends AssignerWithPeriodicWatermarks[BenchmarkMessage] {


  private var currentTimestamp = Long.MinValue

  override def getCurrentWatermark: Watermark = {

    new Watermark(if (currentTimestamp == Long.MinValue) Long.MinValue
    else currentTimestamp - 1)
  }

  override def extractTimestamp(element: BenchmarkMessage, previousElementTimestamp: Long): Long = {

    if (previousElementTimestamp >= this.currentTimestamp)
      this.currentTimestamp = element.inputTS

    element.inputTS
  }

}

private class DataAndTSDeserializer(keyspace: Long) extends KeyedDeserializationSchema[BenchmarkMessage] {
  val typeInformation: TypeInformation[BenchmarkMessage] = createTypeInformation[BenchmarkMessage]

  var msg: BenchmarkMessage = new BenchmarkMessage()

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): BenchmarkMessage = {
    val tokens = new String(message).split(" ")
    msg.inputTS = tokens(0).toLong
    msg.data = tokens(1).toLong
    msg.key = BenchmarkMapper.shuffleKey(msg.data, keyspace)
    //new BenchmarkMessage(tokens(0).toLong, tokens(1).toLong, BenchmarkMapper.shuffleKey(tokens(1).toLong, keyspace))
    msg
  }

  override def isEndOfStream(nextElement: BenchmarkMessage): Boolean = false

  override def getProducedType: TypeInformation[BenchmarkMessage] = typeInformation
}

class DataAndTSSerializer extends SerializationSchema[BenchmarkMessage] {
  var mapper: JsonMapper = _

  override def serialize(element: BenchmarkMessage): Array[Byte] = {
    var b: Array[Byte] = null
    if (mapper == null) {
      mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    }
    try {
      b = mapper.writeValueAsBytes(element)
    } catch {
      case e: JsonProcessingException => return null
    }

    b
    //(element.inputTS + " " + element.data).getBytes()
  }
}

object KafkaSourcesSinks {
  def src(properties: Properties, timeChar: String, parallelism: Int, numArraysToWrite: Long): SourceFunction[BenchmarkMessage] = {
    val inputTopicName = s"benchmark-input"
    properties.setProperty("auto.commit.interval.ms", "500")
    properties.setProperty("enable.auto.commit", "true")
    val cons = new FlinkKafkaConsumer011(inputTopicName, new DataAndTSDeserializer(parallelism * numArraysToWrite), properties)
    if (timeChar.equals("event"))
      cons.assignTimestampsAndWatermarks(new MyExtractor())
    cons.setStartFromEarliest()
    cons.setCommitOffsetsOnCheckpoints(true)
  }

  def sink(properties: Properties, transactional: Boolean): SinkFunction[BenchmarkMessage] = {
    val outputTopicName = s"benchmark-output"
    var prod: FlinkKafkaProducer011[BenchmarkMessage] = null
    //EXACTLY-ONCE Output
    if (transactional)
      prod = new FlinkKafkaProducer011[BenchmarkMessage](outputTopicName, new KeyedSerializationSchemaWrapper[BenchmarkMessage](new DataAndTSSerializer), properties, Semantic.EXACTLY_ONCE)
    else //EXACTLY-ONCE Processing
      prod = new FlinkKafkaProducer011[BenchmarkMessage](outputTopicName, new DataAndTSSerializer, properties)

    prod
  }
}
