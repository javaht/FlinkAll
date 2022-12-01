package com.zht.base.transform.basic;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



public class flatMapTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("datas/clicks.csv");


        ds.flatMap((String line, Collector<String> out) -> {
             String[] fields = line.split(",");
              out.collect(fields[0]);
              out.collect(fields[1]);
              out.collect(fields[2]);
        }).returns(String.class).print();

        env.execute();


    }
}
