package com.zht.ProcessFunction.UnionStream;

import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

//需要两条流的数据类型一样
public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("hadoop102",7777)
                .map(
                        data->{
                            String[] fields = data.split(",");
                            return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
               .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop103",7777)
                .map(
                        data->{
                            String[] fields = data.split(",");
                            return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        //合并两条流

        stream1.union(stream2).process(
                new ProcessFunction<Event, String>() {

                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {

                        out.collect("水位线是："+ctx.timerService().currentWatermark());
                    }
                }
        ).print();


        env.execute("");
    }
}
