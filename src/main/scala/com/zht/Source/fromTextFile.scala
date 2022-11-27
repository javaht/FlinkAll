package com.zht.Source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object fromTextFile {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    val datas: DataStreamSource[String] = env.readTextFile("datas/clicks.csv")

    datas.print()
    env.execute()
  }

}
