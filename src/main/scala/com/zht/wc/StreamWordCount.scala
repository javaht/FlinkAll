package com.zht.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._           //这个是scala的所有隐式转换
/*
* 这个是流处理
* */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val env  = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[String] = env.socketTextStream("192.168.20.62", 7777) //接收socket文本流

    val line: DataStream[String] = inputStream.flatMap(x => x.split(" ")).filter(x=>x.nonEmpty)

    line.map((_,1)).keyBy(0).sum(1).print()

    env.execute("执行个流")


  }
}
