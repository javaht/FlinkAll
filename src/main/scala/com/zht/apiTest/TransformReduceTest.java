package com.zht.apiTest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id==200",3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );
            //统计每个用户的访问频次
/*        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1L);
            }
        });*/

        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser =
                stream.map(data -> Tuple2.of(data.getUser(), 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
                        .keyBy(0).reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));

        clickByUser.keyBy(data->"key").reduce((a,b)->{
            if (a.f1>b.f1){
                return a;
            }else{
                return b;
            }
        }).print();


        env.execute();
    }
}
