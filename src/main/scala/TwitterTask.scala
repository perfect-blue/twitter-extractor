import TwitterSchema._
import Utillities._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.joda.time.format.DateTimeFormat

class TwitterTask(sparkSession: SparkSession, bootstrapServers:String, topic:String,
                  watermarkSize:String, windowSize:String,
                  directory:String, partition:Int, execTime:String) {
  import sparkSession.implicits._


  private val DATE_TIME_PATTERN = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  private val language=sparkSession.udf.register("language",mapLanguage)
  def mapLanguage:String=>String=(column:String)=>{
    val langMap=scala.collection.mutable.Map("am"->"Amharic","ar"->"Arabic","hy"->"Armennian","bn"->"Bengali",
      "bg"->"bulgarian","my"->"burmese","zh"->"Chinese","am"->"Amharic","cs"->"Czech","da"->"Danish",
      "nl"->"Dutch","en"->"English","et"->"Estonian","fi"->"Finnish","fr"->"French","ka"->"Georgian",
      "de"->"German","el"->"Greek","gu"->"Gujarati","ht"->"Haitian","iw"->"Hebrew","hi"->"Hindi",
      "hu"->"Hungarian","is"->"Icelandic","in"->"Indonesian","it"->"Italian","ja"->"Japanese",
      "kn"->"Kannada","km"->"Khmer","ko"->"Korean","lo"->"Lao","lv"->"Latvian","lt"->"Lithuanian",
      "ml"->"Malayalam","dv"->"Maldivian","mr"->"Marathi","ne"->"Nepali","no"->"Norwegian",
      "or"->"Oriya","pa"->"Panjabi","ps"->"Pashto","fa"->"Persian","pl"->"Polish","pt"->"Portuguese",
      "ro"->"Romanian","ru"->"Russian","sr"->"Serbian","sd"->"Sindhi","si"->"Sinhala","sk"->"Slovak",
      "sl"->"Slovenian","ckb"->"Sorani Kurdish","es"->"Spanish","sv"->"Swedish","tl"->"tagalog",
      "ta"->"Tamil","te"->"Telugu","th"->"Thai","bo"->"Tibetan", "tr"->"Turkish","uk"->"Ukrainian",
      "ur"->"Urdu","ug"->"Uyghur","vi"->"Vietnamese","cy"->"Welsh","und"->"undefined","ca"->"French Canada","eu"->"Basque")
    langMap(column)
  }

  private val getVerified=sparkSession.udf.register("getVerified",isVerified)
  def isVerified:Boolean=>Int=(column:Boolean)=>{
    if(column) 1
    else 0
  }


  private val df=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapServers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  //ubah data frame menjadi string
  private val stringDF=df.selectExpr("CAST(Value AS STRING)")
  private val tweetDF=stringDF.select(from_json($"Value",payloadStruct) as("tweet"))

  private val statusDF=tweetDF.selectExpr("tweet.payload.Id",
    "tweet.payload.CreatedAt","tweet.payload.Text","tweet.payload.Source",
    "tweet.payload.FavoriteCount",
    "tweet.payload.Retweet",
    "tweet.payload.RetweetCount",
    "tweet.payload.User.Id AS UserId",
    "tweet.payload.User.FollowersCount",
    "tweet.payload.User.FriendsCount",
    "tweet.payload.User.StatusesCount",
    "tweet.payload.User.verified",
    "tweet.payload.UserMentionEntities",
    "tweet.payload.Lang")
    .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL")



  def subscribe(): Unit ={

    val analyticsWatermark=getAnalyticsWatermark()
      .withColumn("date", col("start").cast(DateType))
      .withColumn("language",language(col("Lang")))

    analyticsWatermark.printSchema()
    val query=writeQueryConsole(analyticsWatermark,"update")
    val csv=analyticsWatermark
      .writeStream
      .partitionBy("Date","PartsOfDay")
      .format("csv")
      .option("header",true)
      .option("truncated",true)
      .option("path", directory+"output/")
      .outputMode(OutputMode.Append)
      .start()

    query.awaitTermination()
    csv.awaitTermination()
  }


  def getAnalyticsWatermark():Dataset[Row]={
    val windowed=statusDF
      .withColumn("tweetLength",getTweetLength(col("Text")))
      .withColumn("CharacterLength",getCharacterLength(col("Text")))
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))
      .withColumn("PositiveTweet",countFavorite(col("FavoriteCount")))
      .withColumn("ViralTweet",countRetweet(col("RetweetCount")))
      .withColumn("InfluencedTweet",countFollower(col("FollowersCount")))
      .withColumn("VerifiedTweet",getVerified(col("verified")))
      .withColumn("RT",countType(col("Retweet")))
      .withColumn("NotRT",notRT(col("Retweet")))
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize),$"Lang")
      .agg(
        expr("bround(avg(tweetLength),3) AS AverageWord"),
        expr("bround(avg(CharacterLength),3) AS AverageCharacter"),
        expr("count(Id) AS CountTweet"),
        expr("bround(avg(FriendsCount),2) AS AverageFriends"),
        expr("bround(avg(StatusesCount),2) AS AverageStatus"),
        expr("sum(PositiveTweet) As PositiveTweet"),
        expr("sum(ViralTweet) AS ViralTweet"),
        expr("sum(InfluencedTweet) AS InfluencedTweet"),
        expr("sum(VerifiedTweet) AS VerifiedTweet"),
        expr("sum(RT) AS Retweet"),
        expr("sum(NotRT) AS PersonalTweet")
      )

    windowed.selectExpr("window.start","window.end","Lang",
      "AverageWord","AverageCharacter", "CountTweet","AverageFriends","AverageStatus",
      "PositiveTweet","ViralTweet","InfluencedTweet","VerifiedTweet","Retweet","PersonalTweet")

  }

  //UDF
  //convert epoch time
  val convertEpoch=sparkSession.udf.register("convertEpoch",convEpoch)
  def convEpoch:String=>Long=(column:String)=>{
    val time=BigInt.apply(column)
    val result=time/1000
    result.toLong
  }

  //UDF
  private val getTweetLength=sparkSession.udf.register("getTweetLength",getLength)
  def getLength:String=> Int =(column:String)=>{
    val textSplit=column.split(" ")
    textSplit.length
  }

  private val getCharacterLength=sparkSession.udf.register("getCharacterLength",CharacterLength)
  def CharacterLength:String=> Int =(column:String)=>{
    column.length()
  }

  private val countFavorite=sparkSession.udf.register("countFavorite",getFavorite)
  def getFavorite:Int=> Int =(column:Int)=>{
    if(column>=50) 1
    else 0
  }

  private val countRetweet=sparkSession.udf.register("getRetweet",getRetweet)
  def getRetweet:Int=> Int=(column:Int)=>{
    if(column>=100) 1
    else 0
  }

  private val countFollower=sparkSession.udf.register("getFollower",getFollower)
  def getFollower:Int=> Int=(column:Int)=>{
    if(column>=3000)1
    else 0
  }

  private val countType=sparkSession.udf.register("getType",getType)
  def getType:Boolean=>Int=(column:Boolean)=>{
    if(column) 1
    else 0
  }

  private val notRT=sparkSession.udf.register("NotRT",getNotRT)
  def getNotRT:Boolean=>Int=(column:Boolean)=>{
    if(column) 0
    else 1
  }

  private val time=sparkSession.udf.register("time",getTime)
  def getTime:String=> String =(column:String)=>{
    val date=DATE_TIME_PATTERN.parseDateTime(column)
    val hour=date.getHourOfDay
    val min=date.getMinuteOfDay
    hour.toString
    if(hour>=7 && hour<=12){
      "morning"
    }else if(hour>12 && hour<=15){
      "afternoon"
    }else if(hour>15 && hour<=18){
      "evening"
    }else if(hour>18 && hour<24){
      "night"
    }else{
      "midnight"
    }

  }

}
