package com.zht.base.transform.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
          //过滤掉奇数，保留偶数
        DataStream<Integer> even = numbers.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0; //过滤掉返回 false 的数组
            }
        });
        env.execute("");

    }
}
