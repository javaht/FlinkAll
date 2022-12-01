package com.zht.window;

import com.zht.base.transform.Event;
import com.zht.window.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //100毫秒触发一次   周期性的生成watermark
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<Event> stream = env.socketTextStream("192.168.20.62", 7777).map(
                (MapFunction<String, Event>) value -> {
                    String[] split = value.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.parseLong(split[2].trim()));
                }
        );


        /*
         * 乱序流的watermark流生成
         * */
        SingleOutputStreamOperator<Event> assign = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));


        stream.print();
        //定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late"){};


        //统计每个url的访问量
        // 三重保证 watermark(水位线) | allowedLateness(允许数据迟到的最大时间)  | sideOutputLateData(侧输出流)
         //比如现在是20秒却来了10秒的数据 此时因为等待时间是1分钟  所以会将10s的数据重新至10s内
        SingleOutputStreamOperator<UrlViewCount> result = assign.keyBy(data -> data.url)
                 .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());
        result.print("result");
        result.getSideOutput(late).print("late");


        env.execute();
    }

    public static  class UrlViewCountAgg implements AggregateFunction<Event,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long,UrlViewCount,String, TimeWindow> {


        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UrlViewCount(s,count,start,end));

        }
    }
}
