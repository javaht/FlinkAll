package com.zht.base.Source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object fromSocket {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)




    env.execute()
  }

}
