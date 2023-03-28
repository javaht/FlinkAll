package com.zht.Stream.ProcessFunction.ProcessWindowFunction;

import com.zht.base.Watermark.ClickSource;
import com.zht.base.transform.Event;
import com.zht.window.aggregateUse;
import com.zht.window.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

public class ProcessWindowFunction2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        //stream.print();

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new aggregateUse.UrlViewCountAgg(), new aggregateUse.UrlViewCountResult());

        urlCountStream.print("Url count");
       urlCountStream.keyBy(data -> data.windowEnd).process(new TopNProcessResult(2)).print();


        env.execute();
    }
//实现自定义的key的processFunction
public static  class TopNProcessResult  extends KeyedProcessFunction<Long,UrlViewCount,String>{
    private Integer n;

    //定义一个列表状态
    private ListState<UrlViewCount>    listState;
    public TopNProcessResult(Integer n) {
        this.n = n;
    }

    /*
    * 生命周期方法
    * */
    @Override
    public void open(Configuration parameters) throws Exception {
        listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-count-list", UrlViewCount.class));//里面是描述器

    }

    @Override
    public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        listState.add(value);

        //WindowEnd+1ms的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

        ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();

          for(UrlViewCount urlviewcount:listState.get()){
              urlViewCountArrayList.add(urlviewcount);
          }
        urlViewCountArrayList.sort((o1,o2)->(o2.count.intValue()-o1.count.intValue()) );

           //包装信息打印输出
        StringBuilder result = new StringBuilder();
        result.append("------------------------------"+"\n");
        result.append("窗口结束时间  "+new Timestamp(ctx.getCurrentKey()) +"\n");
        //取List前两个 包装信息输出
        for(int i=0;i<n;i++){
            UrlViewCount currTuple = urlViewCountArrayList.get(i);
            String info ="No."+(i+1)+"  "+"url: "+currTuple.url+"  "+"访问量："+currTuple.count+"\n";
            result.append(info);

        }
        result.append("------------------------------"+"\n");
        out.collect(result.toString());
    }
}
}
