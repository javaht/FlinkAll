package com.zht.apiTest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties


 /*
 *定义样例类 温度传感器
 * */

case class SensorReading(id: String, timestamp: Long, temperature: Double)
object SourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从集合中读取数据
/*    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    stream1.print()
 */


 /*   val path = "D:\\soft\\idea_workplace\\FlinkWc\\src\\main\\resources\\sensor.txt"
    val stream2: DataStream[String] = env.readTextFile(path)

    stream2.print()
*/

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 =env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties) )

    stream3.print()
    env.execute("source test")



  }


}
