package com.zht.base.sink;

import com.zht.base.transform.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //1.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");


        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //用flink转换处理
        SingleOutputStreamOperator<String> result = kafkaStream.map((MapFunction<String, String>) lines -> {

            String[] fields = lines.split(",");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
        });

         //2.写入kafka
        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}
