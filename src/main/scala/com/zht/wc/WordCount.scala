package com.zht.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._           //这个是scala的所有隐式转换
/*
* 批处理wordcount
* */
object WordCount {
  def main(args: Array[String]): Unit = {

     //创建一个批处理执行环境
    val env =ExecutionEnvironment.getExecutionEnvironment


     //从文件读取数据
      val path = "D:\\soft\\idea_workplace\\FlinkWc\\src\\main\\resources\\hello.txt"

      val dataSet = env.readTextFile(path)
      val value: DataSet[String] = dataSet.flatMap(
        x=>(x.split(" "))
      )

    val result: DataSet[(String, Int)] = value.map(x => (x, 1))
    result.groupBy(0).sum(1).print()        //sum(1) 指的是下标为1



  }

}
