package com.zht.OperatorState;
/*
 * @Author root
 * @Data  2022/5/27 13:38
 * @Description
 * */


import com.zht.transform.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new com.zht.Watermark.ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.print("input");


        //自定义的sink
        stream.addSink(new BufferingSink(10));

        env.execute("BufferingSinkExample");
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
               //定义当前类的属性 批量
        private int threshold;//阈值
        private List<Event> bufferElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferElements=new ArrayList<>();
        }


        //定义算子状态
        private ListState<Event> checkpointedState;


        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferElements.add(value);//缓存到列表中
            //如果列表中的数据大于阈值 就输出
            if(bufferElements.size()>=threshold){
                            for(Event event:bufferElements){
                                System.out.println(event);
                            }
                             System.out.println("输出完毕");
                            bufferElements.clear();
                        }

        }

       // 初始化状态时调用这个方法，也会在恢复状态时调用
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
               checkpointedState.clear();
               //对状态进行持久化 复制缓存的列表到列表状态
            for (Event event:bufferElements){
                checkpointedState.add(event);
            }

        }
        // 保存状态快照到检查点时，调用这个方法
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            //从故障恢复需要将liststate的所有元素恢复到缓存列表中
               if(context.isRestored()){
                   for (Event event:checkpointedState.get()){
                       bufferElements.add(event);
                   }
               }
        }
    }



}
