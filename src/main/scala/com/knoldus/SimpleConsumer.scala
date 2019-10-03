package com.knoldus
import java.util
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
object SimpleConsumer extends App {
  val props = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  val consumer = new KafkaConsumer(props)
  try {
    consumer.subscribe(util.Arrays.asList("knoldus"))
    while (true) {
      val records = consumer.poll(1000).asScala.iterator
      for (value <- records)
        println(value.value())
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
