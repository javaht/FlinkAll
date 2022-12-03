package com.zht.State;

import com.zht.base.Watermark.ClickSource;
import com.zht.base.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class FakeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print();
        stream.keyBy(data->data.url)
                        .process(new FakeWindowResult(1000L)).print();

        env.execute("");
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String,Event,String>{

        private Long windowsize;
        public FakeWindowResult(Long windowsize) {
            this.windowsize =windowsize;
        }

        //定义一个Mapstate
        MapState<Long,Long>  mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("map-state",Long.class,Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            //没来一条数据 根据时间戳判断是哪个窗口
             Long windowStart = value.timestamp / windowsize * windowsize;
             Long windowEnd = windowStart+windowsize;

             ctx.timerService().registerEventTimeTimer(windowEnd-1);

             if(mapState.contains(windowStart)){
                 Long count = mapState.get(windowStart);
                 mapState.put(windowStart,count+1);
             } else {
                 mapState.put(windowStart,1L);
             }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                 Long windowend = timestamp+1;
                 Long windowStart =windowend -windowsize;
                 Long count = mapState.get(windowStart);
            out.collect("窗口" +new Timestamp(windowStart)+"~"+new Timestamp(windowend)+"count"+count);
            mapState.remove(windowStart);
        }
    }

}
