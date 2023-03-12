package com.zht.base.transform.basic;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//该算子只能对 Tuple 类型数据使用，project 方法的功能类似 sql 中的"select 字段"
//该方法只有 Java 的 API 有，Scala 的 API 没此方法
public class project {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, String, Integer>> users = env.fromElements(
                Tuple3.of("佩奇", "女", 5),
                Tuple3.of("乔治", "男", 3)
        );
     users.project(0, 2).print();


    env.execute("");
    }
}
