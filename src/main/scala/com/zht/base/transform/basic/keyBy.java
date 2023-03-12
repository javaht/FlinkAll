package com.zht.base.transform.basic;

import com.zht.base.transform.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class keyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.fromElements(
                new Event("Mary", "./home/3", 1000L * 2),
                new Event("Cary", "./home", 600 * 1000L),
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home/3", 600 * 1000L + 1));

        //按照 Tuple2 中的第 0 个位置进行分组，分组后得到 KeyedStream
        source.keyBy(data->data.getUser());
        //按照 Bean 中的属性名 word 进行分组
        source.keyBy(data->data.getTimestamp());


        env.execute("");
    }
}
