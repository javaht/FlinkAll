package com.zht.SQL;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableAggregateFunctionTest {
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
        tableEnv.createTemporarySystemFunction("MySplit", Top2.class);

        String winAggQuery = "select user,count(url) as cnt,window_start,window_end FROM " +
                " TABLE(tumble(TABLE clickTable,DESCRIPTOR(et),INTERVAL '10' SECOND)) ) "+
                " GROUP BY user,window_start,window_end";
        Table aggTable = tableEnv.sqlQuery(winAggQuery);
        Table resultTable = aggTable.groupBy($("window_end")).flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

          tableEnv.toChangelogStream(resultTable).print();

        env.execute("");
    }

    //单独定义累加器类型  包含当前最大和第二大的数据

    public static class Top2Accumulator{
        public Long max;
        public Long secondMax;
    }

     public static class Top2 extends TableAggregateFunction<Tuple2<Long,Integer>,Top2Accumulator> {


         @Override
         public Top2Accumulator createAccumulator() {

             Top2Accumulator top2Accumulator = new Top2Accumulator();
             top2Accumulator.max = Long.MIN_VALUE;
             top2Accumulator.secondMax = Long.MIN_VALUE;
             return top2Accumulator;
         }
         //定义一个更新累加器的方法
         public void accumulator(Top2Accumulator accumulator,Long value){
                 if(value >accumulator.max){
            accumulator.secondMax = accumulator.max;
            accumulator.max = value;
                 }else if (value>accumulator.secondMax){
                     accumulator.secondMax = value;
                 }
         }

         //输出结果  当前的top2
         public void  emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long,Integer>> out){
                      if(accumulator.max!=Long.MIN_VALUE){
                          out.collect(Tuple2.of(accumulator.max,1));
                      }
             if(accumulator.secondMax!=Long.MIN_VALUE){
                 out.collect(Tuple2.of(accumulator.secondMax,1));
             }

         }

     }

}
