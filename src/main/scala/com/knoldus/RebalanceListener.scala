package com.knoldus

import java.util

import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  KafkaConsumer
}
import org.apache.kafka.common.TopicPartition

class RebalanceListener(consumer: KafkaConsumer[Nothing, Nothing])
    extends ConsumerRebalanceListener {
  override def onPartitionsAssigned(
    partitions: util.Collection[TopicPartition]
  ): Unit = {
    println(" partitions assigned" + partitions)
  }

  override def onPartitionsRevoked(
    partitions: util.Collection[TopicPartition]
  ): Unit = {
    println(" partitions revoked" + partitions)
  }
}
