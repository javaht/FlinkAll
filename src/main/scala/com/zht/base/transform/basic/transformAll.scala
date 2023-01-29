package com.zht.base.transform.basic

import com.zht.base.transform.Event
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
//基本算子
object transformAll {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStreamSource[Event] = env.fromElements(
      new Event("Mary", "./home/3", 1000L * 2),
      new Event("Cary", "./home", 600 * 1000L),
      new Event("Mary", "./home", 1000L),
      new Event("Bob", "./cart", 2000L),
      new Event("Alice", "./prod?id=1", 5 * 1000L),
      new Event("Cary", "./home/3", 600 * 1000L + 1))

    //Map算子的使用
    //stream.map(data=>data.getUser).print()

    //Map算子的使用
    //  stream.map(new MapFunction[Event, String](){
    //    override def map(event: Event): String = {
    //      event.getUrl
    //     }
    //  }).print()

    //FlatMap算子的使用
    //   stream.flatMap(new FlatMapFunction[Event, String]() {
    //     @throws[Exception]
    //     override def flatMap(event: Event, collector: Collector[String]): Unit = {
    //       collector.collect(event.getUser)
    //       collector.collect(event.getUrl)
    //     }
    //   }).print

    //java写法
    //    ds.flatMap((String line, Collector<String> out) -> {
    //      String[] fields = line.split(",");
    //      out.collect(fields[0]);
    //      out.collect(fields[1]);
    //      out.collect(fields[2]);
    //    }).returns(String.class).print();


    //FlatMap算子的使用
    //    stream.flatMap((event: Event,collector: Collector[String])=>{
    //      collector.collect(event.getUser)
    //      collector.collect(event.getUrl)
    //    }).returns(classOf[String]).print()


    // fliter算子的使用
    // stream.filter(key=>key.getUser.equals("Mary")).print()

    // fliter算子的使用
    //    stream.filter(new FilterFunction[Event] {
    //      override def filter(event: Event) = {
    //     event.getUser.equals("Mary")
    //
    //      }
    //    }).returns(classOf[String]).print();



    env.execute()
  }
}
