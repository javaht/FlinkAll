package com.zht.base.Source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FromKafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("hadoop102:9092")
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .setStartingOffsets(OffsetsInitializer.latest).setTopics("flink-01")
      .setGroupId("fk03")
      .build




    env.execute()
  }

}
