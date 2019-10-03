package com.knoldus
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object SimpleProducer extends App {
  val topic = "knoldus"
  val props = new Properties()
  val key = "dev1"
  val value = "interns"
  props.put("bootstrap.servers", "localhost:9092")
  props.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  val producer = new KafkaProducer[String, String](props)
  val record = new ProducerRecord[String, String](topic, key, value)
  try {
    for (i <- 0 to 4) {
      val metadata = producer.send(record).get()
      printf(
        s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
        record.key(),
        record.value(),
        metadata.partition(),
        metadata.offset()
      )
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
