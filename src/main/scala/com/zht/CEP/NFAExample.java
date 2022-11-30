package com.zht.CEP;
/*
 * @Author root
 * @Data  2022/6/2 13:58
 * @Description
 * */

import com.zht.window.entity.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据流
        KeyedStream<OrderEvent, String> stream = env.fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.timestamp))
                .keyBy(data -> data.userId);

        //数据按照顺序依次输入，用状态机进行处理 状态跳转

      //  stream.flatMap(new StateMachineMapper()).print();


        env.execute("");
    }

    @SuppressWarnings("serial")
    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {


        //声明状态机当前的状态
        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {

            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }
            //跳转到下一状态
            State   nextState = state.transition(value.eventType);
            //判断当前状态的特殊情况  直接进行跳转
            if(nextState ==State.Matched){
                //检测到了匹配 输出报警信息
                out.collect(value.userId+"连续三次失败");
            }else if(nextState==State.Terminal){
                //直接将状态更新为初始状态 从新开始检测
                currentState.update(State.Initial);
            }else{
                currentState.update(nextState);
            }
        }
    }

    //状态机
    public enum State {
        Terminal,   //匹配失败 终止状态
        Matched,   //匹配成功
        //S2状态 传入基于S2状态可以进行的一系列状态转移
        S2(new Transition("fail", Matched), new Transition("sucess", Terminal)),


        S1(new Transition("fail", S2), new Transition("sucess", Terminal)),

        //初始状态
        Initial(new Transition("fail", S1), new Transition("sucess", Terminal));

        private Transition[] transitions; //当前转换规则

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        //状态转移方法
        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.eventType.equals(eventType)) {
                    return transition.targetState;
                }
            }
            return Initial;

        }

        //定义一个状态转移类 包含当前引起状态转移的事件类型 以及转移的目标状态
        public static class Transition {
            private String eventType;
            private NFAExample.State targetState;

            public Transition(String eventType, NFAExample.State targetState) {
                this.eventType = eventType;
                this.targetState = targetState;
            }
        }

    }
}

