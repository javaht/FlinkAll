package com.zht.base.Watermark;

import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.time.Duration;


public class FlinkWaterMark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次   周期性的生成watermark


         env.addSource(new com.zht.base.Watermark.ClickSource())


                /*
                 * 水位线靠近源有序流的watermark生成
                 * */
           /*  stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                           .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));
           */

                /*
                 * 乱序流的watermark流生成
                 * */

                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp))

/*
* 滚动处理时间窗口
* */

        .map(data-> Tuple2.of(data.user,1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data->data.f0)
                 // .countWindow(10);//滚动窗口
                //.countWindow(10,2) //滑动计数窗口 传一个参数就是滚动窗口  传两个参数就是滑动窗口


                //.window(EventTimeSessionWindows.withGap(Time.seconds(2))); //会话窗口
               // .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))  //滑动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//滚动事件时间窗口





 /*
 * 规约函数
 * */
        .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0,value1.f1+value2.f1)).print();








        env.execute("SinkToFile");
    }

}
