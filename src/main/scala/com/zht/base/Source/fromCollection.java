package com.zht.base.Source;

import com.zht.base.transform.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class fromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary","./home",1000L));
        clicks.add(new Event("Bob","./cart",2000L));

        DataStream<Event> stream = env.fromCollection(clicks);


        stream.print();

        env.execute();
    }
}
