package com.zht.window;

import com.zht.base.Watermark.ClickSource;
import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class Aggregate_PvUv {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次   周期性的生成watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        //乱序流的watermark流生成
        stream.print();
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp))

        .keyBy(data-> true).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))//滚动事件时间窗口
                .aggregate(new AvgPv())  //窗口函数的聚合函数
                .print();
        env.execute("PVUV");
    }

 public static class AvgPv implements AggregateFunction<Event,Tuple2<Long, HashSet<String>>,Double>{

     @Override
     public Tuple2<Long, HashSet<String>> createAccumulator() {
         return Tuple2.of(0L,new HashSet<String>());
     }

     @Override
     public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {

         //每来一条数据PV个数加1  将User放入hashset中
         accumulator.f1.add(event.user);
         return Tuple2.of(accumulator.f0+1,accumulator.f1);
     }
     @Override
     public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {

         return (double)accumulator.f0/accumulator.f1.size();
     }

     @Override
     public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
         return null;
     }
 }

}
