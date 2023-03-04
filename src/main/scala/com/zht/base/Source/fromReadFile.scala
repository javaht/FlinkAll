package com.zht.base.Source
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object fromReadFile {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
     // PROCESS_ONCE，只读取文件中的数据一次，读取完成后，程序退出
     //PROCESS_CONTINUOUSLY  会一直监听指定的文件，文件的内容发生变化后，会将以前的内容和新的内容全部都读取出来，进而造成数据重复读取。
     //                      一直监听指定的文件或目录，2 秒钟检测一次文件是否发生变化

    val path = "file:///Users/xing/Desktop/a.txt"
    val lines: DataStream[String] = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000)



    env.execute()
  }
}
