import java.util

import TwitterSchema.payloadStruct
import org.apache.spark.ml.feature.{HashingTF, IDF, NGram, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions.{col, expr, from_json, from_unixtime, to_timestamp, window}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer


class FeatureExtraction(sparkSession:SparkSession,df:DataFrame,watermarkSize:String,windowSize:String) {
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
    "tweet.payload.HashtagEntities",
    "tweet.payload.UserMentionEntities",
    "tweet.payload.Lang",
    "tweet.payload.User.Location")
    .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL")


  def getTF():Dataset[Row]={
    val text=statusDF.selectExpr("Id","Text")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("Text")
      .setOutputCol("words")
      .setToLowercase(true)
      .setPattern("(@[A-Za-z0-9]+)|([^A-Za-z])|(\\w+:\\/\\/\\S+)")

    val wordsData = regexTokenizer.transform(text)

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("Filtered")

    //Filter
    val filteredData=remover.transform(wordsData)

    //TF
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    featurizedData
  }

  def getAnalyticsWithWatermark():Dataset[Row]={
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

  def getHashtag():Dataset[Row]={
    val hashtagEntities=statusDF.selectExpr("CreatedAt","Lang","HashtagEntities")
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))

    val hashClass=hashtagEntities.select($"*",functions.explode($"HashtagEntities") as "Hash")
    val hashtag=hashClass.withColumn("Hashtag",$"Hash".getField("Text"))
    val hashtagCount=hashtag.select($"*")
    val windowed=hashtagCount
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize),$"Lang",$"Hashtag")
      .agg(
        expr("count(Hashtag) as CountHashtag")
      )

    windowed.selectExpr("window.start","window.end","Lang","Hashtag","CountHashtag")
  }

  def getWords():Dataset[Row]={
    val text=statusDF.selectExpr("Id","Text","CreatedAt","Lang")
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("Text")
      .setOutputCol("words")
      .setToLowercase(true)
      .setPattern("(@[A-Za-z0-9]+)|([^A-Za-z])|(\\w+:\\/\\/\\S+)")

    val wordsData = regexTokenizer.transform(text)

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("Filtered")

    //Filter
    val filteredData=remover.transform(wordsData)
    val token=filteredData.select($"*",functions.explode($"Words") as("WordToken"))
    val windowed=token
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize),$"Lang",$"WordToken")
      .agg(
        expr("count(WordToken) as CountWords")
      )

    windowed.selectExpr("window.start","window.end","Lang","WordToken","CountWords")
  }

  def getMention():Dataset[Row]={
    val mentionEntities=statusDF.selectExpr("CreatedAt","Lang","UserMentionEntities")
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))

    val mentionClass=mentionEntities.select($"*",functions.explode($"UserMentionEntities") as "MentionEntity")
    val mention=mentionClass.withColumn("Mentions",$"MentionEntity".getField("ScreenName"))

    mention.select($"time",$"MentionEntity",$"Mentions")
    val windowed=mention
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize),$"Lang",$"Mentions")
      .agg(
        expr("count(Mentions) as CountMentions")
      )

    windowed.selectExpr("window.start","window.end","Lang","Mentions","CountMentions")
  }

  def getLocation():Dataset[Row]={
    val location=tweetDF.selectExpr("tweet.payload.User.Location").where("Location IS not null")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("Location")
      .setOutputCol("LocationFeature")
      .setToLowercase(true)
      .setPattern("(@[A-Za-z0-9]+)|([^A-Za-z])|(\\w+:\\/\\/\\S+)")
    val wordsData = regexTokenizer.transform(location)

    val remover = new StopWordsRemover()
      .setInputCol("LocationFeature")
      .setOutputCol("Filtered")

    //Filter
    val filteredData=remover.transform(wordsData)

    val ngram = new NGram().setN(2).setInputCol("Filtered").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(filteredData)
    ngramDataFrame.select("ngrams")

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
    if(column>=10) 1
    else 0
  }

  private val countRetweet=sparkSession.udf.register("getRetweet",getRetweet)
  def getRetweet:Int=> Int=(column:Int)=>{
    if(column>=50) 1
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

  private val getVerified=sparkSession.udf.register("getVerified",isVerified)
  def isVerified:Boolean=>Int=(column:Boolean)=>{
    if(column) 1
    else 0
  }
}
