package com.knoldus

import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object Consumer3 extends App {

  val props: Properties = new Properties()
  props.put("group.id", "ConsumerGroup")
  props.put("bootstrap.servers", "localhost:9092")
  props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  val consumer = new KafkaConsumer(props)
  val rebalanceListner = new RebalanceListener(consumer)
  try {
    consumer.subscribe(util.Arrays.asList("Hello-Kafka1"), rebalanceListner)
    while (true) {
      val records = consumer.poll(1000).asScala.iterator
      for (record <- records) {

        println(
          "Topic: " + record.topic() +
            ",Key: " + record.key() +
            ",Value: " + record.value() +
            ", Offset: " + record.offset() +
            ", Partition: " + record.partition()
        )

        throw new Exception()
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    //consumer.commitAsync()
    //consumer.close()
  }
}
