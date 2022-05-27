package com.zht.KeyState;

import akka.stream.impl.ReducerState;
import com.zht.Watermark.ClickSource;
import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.keyBy(data->data.user).flatMap(new MyFlatMap()).print();
       env.enableCheckpointing(1000);
        env.execute("");
    }

    //实现自定义的FlatMapFunction

    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        /*
        * 定义状态  所有的keyState
        * */
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event,String> myAggregatingState;

        /*
        * 增加一个本地变量进行对比
        * */
        Long count = 0L;


        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event>  valueStateDescriptor= new ValueStateDescriptor<>("my-state", Event.class);
            //根据不同的key 定义不同的状态
            myValueState = getRuntimeContext().getState(valueStateDescriptor);
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list-state",Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map-state",String.class,Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reducing-state", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event(value1.user, value1.url, value2.timestamp);
                }
            }, Event.class));


            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-aggregating-state", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event value, Long accumulator) {
                    return accumulator+1;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "count："+accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a+b;
                }
            }, Long.class));

            StateTtlConfig ttlConfig =StateTtlConfig.newBuilder(Time.hours(1))//机器时间
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   //设置状态更新类型
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//设置状态是否可见
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);//设置状态过期时间
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            System.out.println(myValueState.value());
            myValueState.update(value);
            System.out.println("更新后的"+myValueState.value());;


            myListState.add(value);

            myMapState.put(value.user,myMapState.get(value.user) == null ? 1 : myMapState.get(value.user)+1);
            System.out.println("myMapState:  "+value.user+"    "+myMapState.get(value.user));

            myAggregatingState.add(value);
            System.out.println("myAggregatingState:  "+myAggregatingState.get());



            myReducingState.add(value);
            System.out.println("myReducingState:  "+myReducingState.get());

            count++;
            System.out.println("count:  "+count);


            myValueState.clear();

        }
    }
}
