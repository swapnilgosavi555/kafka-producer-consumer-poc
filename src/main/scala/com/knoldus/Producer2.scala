package com.knoldus
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer2 extends App {

  val props: Properties = new Properties()
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
  val topic = "Hello-Kafka1"
  try {
    for (i <- 0 to 10) {
      val record =
        new ProducerRecord[String, String](topic, "My Site is Knoldus.com " + i)
      val metadata = producer.send(record)
      printf(
        s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
        record.key(),
        record.value(),
        metadata.get().partition(),
        metadata.get().offset()
      )
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
