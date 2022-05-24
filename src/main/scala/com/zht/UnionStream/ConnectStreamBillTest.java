package com.zht.UnionStream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;


/*
* 两个表的类型不同
* */
public class ConnectStreamBillTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


         //来自
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2)
        );

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "sucess", 3000L),
                Tuple4.of("order-3", "third-party", "sucess", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3)
        );

        appStream.keyBy(data->data.f0)
                .connect(thirdpartStream.keyBy(data->data.f0))
                .process(new OrderMatchResult()).print();

        env.execute("");
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>,Tuple4<String,String,String,Long>,String>{
        //定义状态变量 用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEvente;
        private ValueState<Tuple4<String, String, String, Long>> thirdpartState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEvente = getRuntimeContext().getState(new ValueStateDescriptor<>("appEvente", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdpartState = getRuntimeContext().getState(new ValueStateDescriptor<>("thirdpartState", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (thirdpartState.value() != null) {
                out.collect("对账成功: "+value+" "+thirdpartState.value());
                thirdpartState.clear();
            }else {
                appEvente.update(value);
                //注册定时器,开始等待另一条流的数据
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }
        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (appEvente.value() != null) {
                out.collect("对账成功: "+value+" "+appEvente.value());
                appEvente.clear();
            }else {
                thirdpartState.update(value);
                //注册定时器,开始等待另一条流的数据
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
               if(appEvente.value() != null){
                   out.collect("对账失败: "+appEvente.value()+"第三方支付信息未到");
               }
               if(thirdpartState.value() != null) {
                   out.collect("对账失败: "+thirdpartState.value()+"第三方支付信息未到");
               }
            appEvente.clear();
               thirdpartState.clear();

        }
    }
}
