package com.zht.SQL;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class TableEnv {
    public static void main(String[] args) throws Exception {

         //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2. 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'datas/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 3. 表的查询转换
        // 3.1 调用Table API
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob")).select($("user_name"), $("url"));

        tableEnv.createTemporaryView("resultTable", resultTable);

        // 3.2 执行SQL进行表的查询转换
        //Table resultTable2 = tableEnv.sqlQuery("select url, user_name from resultTable");

        // 3.3 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name, COUNT(url) as cnt from clickTable group by user_name");

        tableEnv.toChangelogStream(aggResult).print("agg");

        // 4. 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createOutDDL);

        // 创建一张用于控制台打印输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";

        tableEnv.executeSql(createPrintOutDDL);

        // 5. 输出表
    //    resultTable.executeInsert("outTable");
//        resultTable.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");
        env.execute("");
    }
}
