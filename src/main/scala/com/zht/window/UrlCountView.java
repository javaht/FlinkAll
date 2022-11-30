package com.zht.window;

import com.zht.Watermark.ClickSource;
import com.zht.transform.Event;
import com.zht.window.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UrlCountView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次   周期性的生成watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                /*
                 * 乱序流的watermark流生成
                 * */

                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));


        stream.print();
       //统计每个url的访问量
        stream.keyBy(data->data.url).window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult()).print();




     env.execute();
    }

public static  class UrlViewCountAgg implements AggregateFunction<Event,Long,Long>{

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

public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount,String, TimeWindow>{


    @Override
    public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
        long start = context.window().getStart();
        long end = context.window().getEnd();
        Long count = elements.iterator().next();
        out.collect(new UrlViewCount(s,count,start,end));

    }
}


}
