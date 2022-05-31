package com.zht.UDF;
/*
 * @Author root
 * @Data  2022/5/30 12:08
 * @Description
 * */


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class Udf_AggregateFunction {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), WATERMARK FOR et AS et - INTERVAL '1' SECOND ) " +
                " WITH ( 'connector' = 'filesystem','path' = 'datas/clicks.csv', 'format' =  'csv' )";

          tableEnv.executeSql(createDDL);

         tableEnv.createTemporarySystemFunction("WeightAvg",WeightAvg.class);
        Table table = tableEnv.sqlQuery("select user_name,WeightAvg(ts,1) from clickTable group by user_name");
        tableEnv.toChangelogStream(table).print();

        env.execute("");
    }
    //单独定义一个累加器类型
    public  static  class WeightAvgAcc{
        public long sum  =0;
        public int count =0;
    }
       //实现自定义的聚合函数  计算加权平均值
    public  static class WeightAvg extends AggregateFunction<Long,WeightAvgAcc>{

           @Override
           public Long getValue(WeightAvgAcc accumulator) {
               if(accumulator.count ==0){
                   return null;
               }else{
                   return accumulator.sum/accumulator.count;}
           }

           @Override
           public WeightAvgAcc createAccumulator() {
               return new WeightAvgAcc();
           }

           //必须手动写 名字必须叫做accumulate
           public void accumulate(WeightAvgAcc accumulator,Long value,Integer weight){//value是输入的值  weight是权重
               accumulator.sum = value * weight;
               accumulator.count += weight;

           }

       }
}
