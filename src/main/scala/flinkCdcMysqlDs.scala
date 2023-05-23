import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @Title:
 * @Desc:
 * @Author: zhangzhequn
 * @Date:Created in 2023/5/23 20:54
 * @TODO:
 * @Other:
 */
object flinkCdcMysqlDs {
  def main(args: Array[String]): Unit = {

    val value: DebeziumSourceFunction[String] = MySqlSource
      .builder()
      .hostname("101.42.12.218")
      .port(3306)
      .databaseList("flink_cdc") // set captured database
      //.tableList("flink_cdc_table") // set captured table
      .username("root")
      .password("Admin@123!")
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
      .build()

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.addSource(value).print.setParallelism(1);
    environment.execute("Print MySQL Snapshot + Binlog")
  }

}
