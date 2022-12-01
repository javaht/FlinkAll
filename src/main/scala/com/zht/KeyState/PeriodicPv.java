package com.zht.KeyState;
/*
 * @Author root
 * @Data  2022/5/26 11:14
 * @Description
 * */


import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPv {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new com.zht.base.Watermark.ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.print("input");
        //统计每个用户的PV
        stream.keyBy(data->data.user)
                        .process(new KeyedProcessFunction<String, Event, String>() {
                            //定义状态 保存当前pv统计值
                            ValueState<Long> countState;
                            ValueState<Long> timerState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState",Long.class));
                                timerState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState",Long.class));

                            }


                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                            //每来一条数据 更新对应的count值
                                Long count = countState.value();
                                countState.update(count==null ? 1 : count+1);

                                //如果没有注册过的话 才去注册
                                if(timerState.value()==null){
                                    ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                                    timerState.update(value.timestamp+10*1000L);
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                 //定时器触发输出结果
                                out.collect("user:"+ctx.getCurrentKey()+" PV:"+countState.value());
                                //清空状态
                                 timerState.clear();
                            }
                        }).print();




        env.execute("PeriodicPv");
    }

public  static class PeriodicPvResult extends KeyedProcessFunction<String,Event,String>{
    //定义状态 保存当前pv统计值
    ValueState<Long> countState;
    ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState",Long.class));

    }


    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

    }

}
}
