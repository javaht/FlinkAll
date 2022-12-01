package com.zht.base.transform.partition;

import com.zht.base.transform.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Mary", "./home", 5000L),
                new Event("Bob", "./cart", 6000L),
                new Event("Alice", "./prod?id=100", 7000L),
                new Event("Bob", "./prod?id=1", 8000L)
        );

        //1.随机分区
      // stream.shuffle().print().setParallelism(4);

        //2.轮询分区
       //stream.rebalance().print().setParallelism(4);


        //3.重缩放分区
//        DataStreamSource<Integer> ds = env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> ctx) throws Exception {
//                for (int i = 1; i <= 8; i++) {
//                    //将奇偶分区
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        System.out.println("生命周期开始，子任务索引是：" + getRuntimeContext().getIndexOfThisSubtask());
//                        ctx.collect(i);
//                    }
//                }
//            }
//            @Override
//            public void cancel() {}
//        }).setParallelism(2);
//        ds.rescale().print().setParallelism(2);





        //4.广播
      // stream.broadcast().print().setParallelism(4);

        /*
        * 5.全局分区 把全部分区设置为第一个分区
        * */
       // stream.global().print().setParallelism(2);


        /*
        * 自定义分区
        * */

//        env.fromElements(1,2,3,4,5,6,7,8)
//                .partitionCustom(new Partitioner<Integer>() {
//                    @Override
//                    public int partition(Integer key, int i) {
//                        return  key % 2;
//                    }
//                }, new KeySelector<Integer, Integer>() {
//                    @Override
//                    public Integer getKey(Integer value) throws Exception {
//                        return value;
//                    }
//                })
//                        .print().setParallelism(4);





        env.execute("TransFormPartitionTest");

    }
}
