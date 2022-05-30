package com.zht.SQL;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HIveConnect {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 配置hive方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            // 配置default方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String name            = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir     = "/opt/hive-conf";

// 创建一个HiveCatalog，并在表环境中注册
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

// 使用HiveCatalog作为当前会话的catalog
        tableEnv.useCatalog("myhive");


    }
}
