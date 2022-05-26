package com.zht.KeyState;
/*
 * @Author root
 * @Data  2022/5/26 13:36
 * @Description
 * */


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamFullJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(Tuple3.of("a", "stream-1", 1000L),
                              Tuple3.of("b", "stream-1", 2000L),
                              Tuple3.of("b", "stream-1", 3000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L),
                        Tuple3.of("a", "stream-1", 6000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2)
                );
 
         
        //自定义列表状态进行全外连结
        stream1.keyBy(data->data.f0)
                        .connect(stream2.keyBy(data->data.f0))
                        .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                            //定义列表状态 用于保存两条流中已经到达的所有数据
                            private ListState<Tuple2<String, Long>> stream1ListState;
                            private ListState<Tuple2<String, Long>> stream2ListState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                stream1ListState= getRuntimeContext().getListState(new ListStateDescriptor<>("stream1ListState", Types.TUPLE(Types.STRING,Types.LONG)));
                                stream2ListState= getRuntimeContext().getListState(new ListStateDescriptor<>("stream2ListState", Types.TUPLE(Types.STRING,Types.LONG)));
                            }

                            @Override
                        public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                            //获取另一条流中所有数据 配对输出
                                for(Tuple2<String, Long> right:stream2ListState.get()){
                                    out.collect(left.f0+"   "+left.f2+"=====>"+right);

                                }
                                stream1ListState.add(Tuple2.of(left.f0,left.f2));

                             }




                        @Override
                        public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                            for(Tuple2<String, Long> left:stream1ListState.get()){
                                out.collect(left+"=====>"+right.f0+"  "+right.f2);

                            }
                            stream2ListState.add(Tuple2.of(right.f0,right.f2));

                        }
             }).print();
                



        env.execute("TwoStreamFullJoin");
    }
}
