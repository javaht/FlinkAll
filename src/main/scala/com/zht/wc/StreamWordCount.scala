package com.zht.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._           //这个是scala的所有隐式转换
/*
* 这个是流处理
* */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool =  ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    env.setParallelism(1)//设置核数为1  并行度为1
    val inputStream: DataStream[String] = env.socketTextStream(host, port) //接收socket文本流

    val line: DataStream[String] = inputStream.flatMap(x => x.split(" ")).filter(x=>x.nonEmpty)

    line.map((_,1)).keyBy(0).sum(1).print()

    env.execute("执行个流")


  }
}
