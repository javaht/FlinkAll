package com.zht.UDF;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class scalaFunction {
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


        //注册自定义函数 标量函数
         tableEnv.createTemporarySystemFunction("MyHash",MyHashFunction.class);
        //调用UDF进行查询转换
        Table table = tableEnv.sqlQuery("select user_name,MyHash(user_name) from clickTable");
        tableEnv.toDataStream(table).print();


        //转换为流打印



        env.execute("");
    }

    public static class MyHashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}



