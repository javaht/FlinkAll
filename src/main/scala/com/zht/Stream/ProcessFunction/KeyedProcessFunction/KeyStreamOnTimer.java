package com.zht.Stream.ProcessFunction.KeyedProcessFunction;

import com.zht.base.Watermark.ClickSource;
import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class KeyStreamOnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));


        stream.keyBy(data->data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

                        long currTs = ctx.timestamp();

//                          System.out.println("这个A时间是    "+new Timestamp(currTs));
//                          System.out.println("这个B时间是    "+new Time(ctx.timerService().currentProcessingTime()));

                        out.collect(ctx.getCurrentKey()+"数据到达时间："+new Timestamp(currTs)+"  当前的watermark；"+new Timestamp(ctx.timerService().currentWatermark()));
                        //注册一个10s后的定时器
                        ctx.timerService().registerEventTimeTimer(currTs+10000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey()+"定时器触发时间："+new Timestamp(timestamp)+"  当前的watermark："+new Timestamp(ctx.timerService().currentWatermark()));
                    }
                }).print();


        env.execute();
    }


}
