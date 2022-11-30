package com.zht.window;

import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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

public class WithAggregate {
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



                //使用AggregateFunction和processlistWindowFunciton
                .keyBy(data -> true).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                        .aggregate(new Uvagg(), new UvCountResult()).print();
        env.execute();
    }


    /*
    * 自定义实现使用AggregateFunction  增量聚合计算uv值
    * */
 public  static class  Uvagg implements AggregateFunction<Event, HashSet<String> ,Long>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> accumulator) {
            accumulator.add(event.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return  (long)accumulator.size(); //这里把这里的结果传给下一个窗口函数
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }


    /*
    * 自定义实现使用ProcessWindowFunction   包装窗口信息输出
    * */
     //这里的key是boolean的原因是 .keyBy(data -> true)
 public static class UvCountResult extends ProcessWindowFunction<Long,String,Boolean, TimeWindow> {


        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long uv =elements.iterator().next();
            out.collect("窗口"+new Timestamp(start)+"-"+new Timestamp(end)+"   "+"uv值为"+uv);
        }
    }
}
