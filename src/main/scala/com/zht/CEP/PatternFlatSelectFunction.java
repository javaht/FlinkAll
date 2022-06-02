package com.zht.CEP;
/*
 * @Author root
 * @Data  2022/6/2 10:01
 * @Description
 * */


import com.zht.window.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class PatternFlatSelectFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.timestamp));


        //定义模式 严格近邻连续三次失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .times(3).consecutive();
        //将模式应用到数据流上 检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream.keyBy(data -> data.userId), pattern);

        patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {

                LoginEvent firstfail = map.get("fail").get(0);
                LoginEvent secondfail = map.get("fail").get(1);
                LoginEvent thirdfail = map.get("fail").get(2);

                String result = firstfail.userId + " " + firstfail.eventType + " " + firstfail.timestamp + " " + secondfail.timestamp + " " + thirdfail.timestamp;
                collector.collect(result);

            }
        }).print();



        env.execute("CEP Test");
    }

}
