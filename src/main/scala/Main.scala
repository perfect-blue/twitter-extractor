import Utillities.setupLogging
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    //buat spark session
    val spark = SparkSession
      .builder()
      .appName("twitter-structured-streaming")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation","/home/hduser/Documents/project/spark/dump/checkpoint/")
      .getOrCreate()

    val df=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","127.0.0.1:9092")
      .option("subscribe","twitter-test-5")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    setupLogging()
    val fe=new FeatureExtraction(spark,df,"1 hours","1 hours")

    val TF=fe.getTF()
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .start()

    val analytics=fe.getAnalyticsWithWatermark()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    val hashtag=fe.getHashtag()
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("complete")
      .start()

    val words=fe.getWords()
      .writeStream
      .format("console")
      .option("truncate",false)
      .outputMode("complete")
      .start()

    val mention=fe.getMention()
        .writeStream
        .format("console")
        .option("truncate",false)
        .outputMode("complete")
        .start()

    val user=fe.getLocation()
      .writeStream
      .format("console")
      .option("truncate",false)
      .outputMode("append")
      .start()

    words.awaitTermination()
    hashtag.awaitTermination()
    mention.awaitTermination()
    analytics.awaitTermination()
    user.awaitTermination()
    TF.awaitTermination()
  }

  def initializeSpark(name:String,directory:String): SparkSession={

    val session = SparkSession
      .builder()
      .appName(name)
      .config("spark.sql.streaming.checkpointLocation", directory+"checkpoint/")
      .getOrCreate()

    session;
  }

}
