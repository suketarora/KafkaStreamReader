import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession

object Main extends App {
   override def main(arg: Array[String]): Unit = {
     val spark =SparkSession.builder().appName("FlightsProducer").master("local[2]").getOrCreate()
  val sc =spark.sparkContext



  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()

  val query = df.writeStream
    .outputMode("update")
    .format("console")
    .trigger(Trigger.ProcessingTime(10000))
//          .option("batch.size",10000)
    .start()


  query.awaitTermination();
   }
}