package com.zht.window;

import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次   周期性的生成watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new com.zht.Watermark.ClickSource());
        /*
         * 乱序流的watermark流生成
         * */
        stream.print();
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp))

                //使用processlistWindowFunciton
                .keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow()).print();
        env.execute();
    }


   //实现自定义的processwindowFunction
     public static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean,TimeWindow>{

       @Override
       public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

            //用一个hashset保存user
           HashSet<String> userset = new HashSet<>();
           for(Event event:elements){
               userset.add(event.user);
           }

            Integer uv = userset.size();

           //结合窗口信息
           Long start  =context.window().getStart();
           Long end = context.window().getEnd();
           out.collect("窗口："+new Timestamp(start)+"---"+new Timestamp(end)+"UV："+uv);

       }
   }

}
