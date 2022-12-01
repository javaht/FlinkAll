package com.zht.ProcessFunction.Function;

import com.zht.base.Watermark.ClickSource;
import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));


                stream.process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        if(("Mary").equals(value.user)){
                            out.collect(value.user+"clicks"+value.url);
                        }else if(value.user.equals("Bob")){
                            out.collect(value.user);
                        }
                        out.collect(value.toString());

                        System.out.println("timestamp: "+ctx.timestamp());
                        System.out.println("当前的watermark："+ctx.timerService().currentWatermark());
                        System.out.println("getRuntimeContext().getIndexOfThisSubtask()   "+getRuntimeContext().getIndexOfThisSubtask());
                    }

                }).print();


        env.execute();
    }


}
