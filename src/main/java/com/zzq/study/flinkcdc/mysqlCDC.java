package com.zzq.study.flinkcdc;


import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Title:
 * @Desc:
 * @Author: zhangzhequn
 * @Date:Created in 2023/5/23 10:08
 * @TODO:
 * @Other:
 */
public class mysqlCDC {
    public static void main(String[] args) throws Exception {


        SourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("101.42.12.218")
                .port(3306)
                .databaseList("flink_cdc") // set captured database
                //.tableList("flink_cdc_table") // set captured table
                .username("root")
                .password("Admin@123!")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(mySqlSource).print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");





    }

}
