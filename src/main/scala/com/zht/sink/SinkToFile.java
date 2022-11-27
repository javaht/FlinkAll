package com.zht.sink;

import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFile {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home/3", 1000L*2),
                new Event("Cary", "./home", 600 * 1000L),
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home/3", 600 * 1000L+1)

        );
        stream.assignTimestampsAndWatermarks( new WatermarkStrategy<Event> (){
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                return null;
            }

            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return WatermarkStrategy.super.createTimestampAssigner(context);
            }
        });




        StreamingFileSink<String> build = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder().withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)).build()
                ).build();

        stream.map(data->data.toString()).addSink(build);

        env.execute("SinkToFile");

    }
}
