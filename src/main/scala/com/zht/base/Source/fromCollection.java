package com.zht.base.Source;

import com.zht.base.transform.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
//非并行的 Source，可以将一个 Collection 作为参数传入到该方法中，返回一个 DataStreamSource
public class fromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        env.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary","./home",1000L));
        clicks.add(new Event("Bob","./cart",2000L));

        DataStream<Event> stream = env.fromCollection(clicks);


        stream.print();
        env.execute();
    }
}
