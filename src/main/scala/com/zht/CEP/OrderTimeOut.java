package com.zht.CEP;
/*
 * @Author root
 * @Data  2022/6/2 11:09
 * @Description
 * */


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class OrderTimeOut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
         //获取数据流
        SingleOutputStreamOperator<OrderEvent> stream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.timestamp));


        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("creat")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(stream.keyBy(data -> data.orderId), pattern);


         //定义一个测输出流的标签
        OutputTag<String> timeputTag = new OutputTag<String>("timeout") {
        };

        patternStream.process(new OrderPatMatch()).print("payed:  ");
        patternStream.process(new OrderPatMatch()).getSideOutput(timeputTag).print("timeout:  ");



        env.execute("");

    }

    public static class OrderPatMatch extends PatternProcessFunction<OrderEvent,String> implements TimedOutPartialMatchHandler<OrderEvent> {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {

            OrderEvent pay = map.get("pay").get(0);
            collector.collect("用户："+pay.userId+" "+pay.orderId+"支付成功"+ pay.timestamp+"支付时间");


        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            OrderEvent creatEvent = map.get("creat").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};
            context.output(timeoutTag,"用户："+creatEvent.userId+"超时id： "+creatEvent.orderId+"超时"+creatEvent.timestamp);
        }
    }


}
