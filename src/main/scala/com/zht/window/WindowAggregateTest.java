package com.zht.window;

import com.zht.Watermark.ClickSource;
import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次   周期性的生成watermark

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())

        /*
         * 乱序流的watermark流生成
         * */

        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));

        /*
         * 滚动处理时间窗口
         * */

        stream.keyBy(data-> data.user).window(TumblingEventTimeWindows.of(Time.seconds(10)))//滚动事件时间窗口
        /*
         * 窗口函数的聚合函数
         * */
                .aggregate(new AggregateFunction<Event, Tuple2<Long,Integer>, String>() {


                    @Override
                    //创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L,0);
                    }

                    @Override
                    //将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进一步聚合的过程。
                    // 方法传入两个参数：当前新到的数据 value，和当前的累加器 accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之后都会调用这个方法。
                    public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0+1,accumulator.f1+1);
                    }


                    @Override
                    //从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，然后再基于这些聚合的状态计算出一个结果进行输出。
                    // 比如之前我们提到的计算平均值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终结果。这个方法只在窗口要输出结果时调用。
                    public String getResult(Tuple2<Long, Integer> accumulator) {
                        Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);//获取结果
                        return timestamp.toString();
                    }

                    @Override
                    //合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用；
                    // 最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {

                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                    }
                }).print();








        env.execute("SinkToFile");
    }

}
