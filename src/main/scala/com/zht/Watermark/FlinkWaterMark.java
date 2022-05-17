package com.zht.WaterMark;

import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class FlinkWaterMark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);//100毫秒触发一次
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Mary", "./home/3", 1000L*2),
                new Event("Cary", "./home", 600 * 1000L),
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home/3", 600 * 1000L+1)

        );


        /*
        * 水位线靠近源    有序流的watermark生成
        * */
     /*  stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));
*/

         /*
         * 乱序流的watermark流生成
         * */

        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, recordtimestamp) -> event.timestamp));



/*
* 滚动处理时间窗口
* */
        stream.keyBy(data->data.user);
                //.countWindow(10);//滚动窗口
                // .countWindow(10,2) //滑动计数窗口 传一个参数就是滚动窗口  传两个参数就是滑动窗口
                //.window(EventTimeSessionWindows.withGap(Time.seconds(2))); //会话窗口
                //.window(SlidingProcessingTimeWindows.of(Time.hours(1),Time.minutes(5)))  //滑动事件时间窗口
                //.window(TumblingProcessingTimeWindows.of(Time.hours(1))); //滚动事件时间窗口


/*
* 窗口函数
* */



        env.execute("SinkToFile");
    }

}
