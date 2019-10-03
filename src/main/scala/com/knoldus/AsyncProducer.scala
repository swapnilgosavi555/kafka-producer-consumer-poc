package com.knoldus

import java.util.Properties

import org.apache.kafka.clients.producer.{
  Callback,
  KafkaProducer,
  ProducerRecord,
  RecordMetadata
}

class AsyncProducer extends Callback {
  def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
    if (e != null)
      println("AsynchronousProducer failed with an exception")
    else
      println("AsynchronousProducer call Success:")
  }
}
object AsyncProducer extends App {
  val topic = "knoldus"
  val props = new Properties()
  val key = "dev1"
  val value = "from async producer"
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
      val metadata = producer.send(record, new AsyncProducer)
      printf(
        s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
        record.key(),
        record.value(),
        metadata.get.partition(),
        metadata.get.offset()
      )
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
