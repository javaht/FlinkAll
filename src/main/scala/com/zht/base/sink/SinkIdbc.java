package com.zht.base.sink;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.zht.base.transform.Event;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class SinkIdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setParallelism(2);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));
        stream.addSink(JdbcSink.exactlyOnceSink(
                "insert into Event (user,url,timestamp) values(?,?,?)",
                (ps, event) -> {
                    ps.setString(1, event.user);
                    ps.setString(2, event.url);
                    ps.setLong(3,event.timestamp);
                },
                JdbcExecutionOptions.builder().withMaxRetries(0).build(),
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
                () -> {
                    MysqlXADataSource xaDataSource = new MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://hadoop102:3306/maxwell?useSSL=false&characterEncoding=utf8&serverTimezone=UTC");
                    xaDataSource.setUser("root");
                    xaDataSource.setPassword("123456");
                    return xaDataSource;
                }
        )).name("sinkTomysql");

        // XADataSource 就是 jdbc 连接，不过它是支持分布式事务的连接
       // 而且它的构造方法，不同的数据库构造方法不同

        env.execute("sink mysql");
    }
}