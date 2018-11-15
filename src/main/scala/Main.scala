import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 



object Main extends App {


  
    override def main(arg: Array[String]): Unit = {
  
      
       val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]").set( "spark.driver.memory", "4g" )  
       val ssc = new StreamingContext(conf, Seconds(10))
       
       val kafkaParams = Map[String, Object](
                    "bootstrap.servers" -> "localhost:9092",  //localhost:9092,kafka-broker1:9092
                    "key.deserializer" -> classOf[StringDeserializer],
                    "value.deserializer" -> classOf[StringDeserializer],
                    "group.id" -> "group one",   //use_a_separate_group_id_for_each_stream
                    "auto.offset.reset" -> "latest",
                    "enable.auto.commit" -> (false: java.lang.Boolean)
                  )

        val topics = Array("test")



        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
          
        val streamTuple = stream.map(record => (record.key, record.value))
        streamTuple.print()
        ssc.start()
        ssc.awaitTermination()
    }

}

