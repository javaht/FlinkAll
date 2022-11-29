package com.zht.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//富函数类中包含包含运行环境的上下文,并且拥有一些生命周期的方法
//典型的生命周期的方法有open close
// open()    初始化方法
// close()   生命周期中最后一个调用的方法
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=100", 2000L)
        );

        stream.map(new MyRitchMapper()).print();

        env.execute();
    }

    public static class MyRitchMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("方法初始化");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        @Override
        public void close() throws Exception {
            System.out.println("方法结束啦");
        }
    }
}
