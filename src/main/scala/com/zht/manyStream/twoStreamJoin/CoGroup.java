package com.zht.manyStream.twoStreamJoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
//          该接口需要重写两个方法：
//map1 方法，是对第 1 个流进行 map 的处理逻辑。
//map2 方法，是对 2 个流进行 map 的处理逻辑
//        这两个方法必须是相同的返回值类型


//还有coFlatMap
public class CoGroup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("b", 5100L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (element, recordTimestamp) -> element.f1));

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("a", 3000),
                Tuple2.of("b", 4000),
                Tuple2.of("a", 4500),
                Tuple2.of("b", 5500)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Integer>>) (element, recordTimestamp) -> element.f1));

    stream1.coGroup(stream2)
                    .where(data->data.f0)
                     .equalTo(data->data.f0)
                     .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                     .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                        @Override
                        public void coGroup(Iterable<Tuple2<String, Long>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {

                            out.collect("first:"+first+"======>"+"second:"+second);
                        }
                     }).print();




        env.execute("");
    }
}
