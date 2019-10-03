package com.knoldus
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
object CustomProducer extends App {

  val props = new Properties()

  val topicName = " Custom-Partitioner"
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put("partitioner.class", "com.knoldus.SensorPartitioner1")
  props.put("speed.sensor.name", "TSS")

  val producer = new KafkaProducer[String, String](props)
  val record = new ProducerRecord[String, String](topicName, "SSP", "swapnil")
  val record1 = new ProducerRecord[String, String](topicName, "TSS", "gosavi")
  for (i <- 1 to 10) {
    val metadata = producer.send(record)
    printf(
      s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)",
      record.key(),
      record.value(),
      /*metadata.get.partition(),
      metadata.get.offset()*/
    )
  }
  /* for (i <- 1 to 3) {
    val metadata = producer.send(record1)
    printf(
      s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
      record.key(),
      record.value(),
      metadata.get.partition(),
      metadata.get.offset()
    )
   */
  // }
}
