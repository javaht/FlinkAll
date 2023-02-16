package com.zht.base.transform.aggregation;

import com.zht.base.transform.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByandMax {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Cary", "./home", 2000L),
                new Event("Mary", "./home/3", 3000L),
                new Event("Bob", "./cart", 4000L),
                new Event("Alice", "./prod?id=1", 5000L),
                new Event("Cary", "./home/3", 6000L));

        //按键分组之后进行聚合 提取当前用户最近一次访问数据
        SingleOutputStreamOperator<Event> timestamp = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp");

        timestamp.print("keyBy算子");

        //max    在输出流上对指定的字段求最大值只改变max中的字段 其他的字段不修改
        //maxBy  在输出流上对指定的字段求最大值 包含字段最大值的整条数据
        //min
        //minBy

         //stream.keyBy(data -> data.user).maxBy("timestamp").print();

        env.execute("TransformSimpleAggTest");
    }
}
