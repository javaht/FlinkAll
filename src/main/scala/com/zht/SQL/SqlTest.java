package com.zht.SQL;

import com.zht.Watermark.ClickSource;
import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        //获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

         //流转换为表
        Table table = tableEnv.fromDataStream(stream);
        Table select = table.select($("url"), $("user"));
         tableEnv.toDataStream(select).print();


        //  Table visitTable = tableEnv.sqlQuery("select  user,url,`timestamp` from " + table);
       //  tableEnv.toDataStream(visitTable).print();

        env.execute("");




    }
}
