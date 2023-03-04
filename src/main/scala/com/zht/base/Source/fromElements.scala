package com.zht.base.Source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//非并行的 Source，可以将一到多个数据作为可变参数传入到该方法中，返回 DataStreamSource
object fromElements {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val words: DataStream[String] = env.fromElements("flink", "hadoop", "spark")


    words.print()


    env.execute("")

  }
}
