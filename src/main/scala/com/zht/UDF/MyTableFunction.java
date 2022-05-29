package com.zht.UDF;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MyTableFunction {
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


        tableEnv.createTemporarySystemFunction("MySplit",MyTableFunctions.class);


        Table resultTable = tableEnv.sqlQuery(" select user_name,url,word,length from clickTable,LATERAL TABLE(MySplit(url)) AS T(word,length)");
        tableEnv.toDataStream(resultTable).print();


        env.execute("");

    }

    public static class MyTableFunctions extends TableFunction<Tuple2<String,Integer>>{

         public void eval(String str){
             String[] fields = str.split("\\?");
             for(String field:fields){
                 collect(Tuple2.of(field,field.length()));
             }
         }
    }
}
