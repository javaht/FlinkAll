package com.zht.base.sink;

import com.zht.base.transform.Event;
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
         //2.为了得到并传入SinkFunction，需要构建StreamingFileSink的一个对象
        //调用forRowFormat方法或者forBulkformat方法得到一个DefaultRowFormatBuilder
        //  其中forBulkformat方法前面还有类型参数，以及传参要求一个目录名称，一个编码器
        //写入文件需要序列化，需要定义序列化方法并进行编码转换，当成Stream写入文件
        //然后再使用builder创建实例
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(//指定滚动策略，根据事件或者文件大小新产生文件归档保存
                        DefaultRollingPolicy.builder()//使用builder构建实例
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))//事件间隔毫秒数
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(15))//当前不活跃的间隔事件，隔多长事件没有数据到来
                                .build()
                ).build();
        stream.map(data->data.toString()).addSink(streamingFileSink);

        env.execute("SinkToFile");

    }
}
