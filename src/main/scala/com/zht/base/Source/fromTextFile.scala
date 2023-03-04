package com.zht.base.Source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object fromTextFile {
  def main(args: Array[String]): Unit = {
    //可以从指定的目录或文件读取数据，默认使用的是 TextInputFormat 格式读取数据，
    //还有一个重载的方法 readTextFile(String filePath, String charsetName)可以传入读取文件指定的字符集，默认是 UTF-8 编码。
    //该方法是一个有限的数据源，数据读完后，程序就会退出，不能一直运行。该方法底层调用的是 readFile 方法，FileProcessingMode 为 PROCESS_ONCE

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    val datas: DataStreamSource[String] = env.readTextFile("datas/clicks.csv")

    datas.print()
    env.execute()
  }

}
