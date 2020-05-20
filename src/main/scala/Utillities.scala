import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object Utillities {

  /**
   * konfigurasi logger sehingga hanya menampilkan pesan ERROR saja
   * untuk menghindari log spam
   */
  def setupLogging()={
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.ERROR)
  }

  def writeQueryConsole(dataFrame: DataFrame,mode:String):StreamingQuery={
    val result=dataFrame.writeStream
      .outputMode(mode)
      .format("console")
      .option("truncated",false)
      .start()

    result
  }

  //TODO: CSV file Still Empty, FIX IT!
  def writeQueryCSV(dataFrame: DataFrame,path:String,directory:String,processing:String):StreamingQuery={
    val result=dataFrame.writeStream
      .format("csv")
      .option("header",true)
      .option("truncated",true)
      .option("path", directory+"output/"+path)
      .outputMode(OutputMode.Append)
//      .trigger(Trigger.ProcessingTime(processing))
      .start()

    result
  }

  //TODO: KAFKA WRITE ON SCHEMA

  def writeQueryKafka(dataFrame:DataFrame,topic:String,hostPort:String):StreamingQuery={
    val topic_df=dataFrame.select(to_json(struct(dataFrame.col("start")).as("key")),
      to_json(dataFrame.col("*")).as("value"))
    val kafkaOutput = topic_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", hostPort)
      .option("topic", topic)
      .outputMode("update")
      .start()

    kafkaOutput
  }

}
