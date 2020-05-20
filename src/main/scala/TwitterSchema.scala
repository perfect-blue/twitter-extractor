import org.apache.spark.sql.types.{DataTypes, LongType, StructType}

object TwitterSchema {

  //place filed
  val PLACE_FIELD = "Place"
  val STREET_ADDRESS_FIELD="StreetAddress"
  val COUNTRY_CODE_FIELD="CountryCode"
  val ID_FIELD="Id"
  val COUNTRY_FIELD="Country"
  val PLACE_TYPE_FIELD="PlaceType"
  val URL_FIELD="URL"
  val FULL_NAME_FIELD="FullName"

  //geolocation field
  val GEO_LOCATION="GeoLocation"
  val LATITUDE_FIELD="Latitude"
  val LONGITUDE_FIELD="longitude"

  //status deletion notice
  val STATUS_DELETION_FIELD="StatusId"
  val USER_DELETION_FIELD="UserId"

  //status field
  val CREATED_AT_FIELD="CreatedAt"
  val TEXT_FIELD="Text"
  val SOURCE_FIELD="Source"
  val TRUNCATED_FIELD="Truncated"
  val IN_REPLY_TO_STATUS_ID="InReplyToStatusId"
  val IN_REPLY_TO_USER_ID="InReplyToUserId"
  val IN_REPLY_TO_SCREEN_NAME="InReplyToScreenName"
  val FAVORITED_FIELD="Favorited"
  val RETWEETED_FIELD="Retweeted"
  val FAVORITE_COUNT_FIELD="FavoriteCount"
  val RETWEET_FIELD="Retweet"
  val CONTRIBUTORS_FIELD="Contributors"
  val RETWEET_COUNT_FIELD="RetweetCount"
  val RETWEETED_BY_ME_FIELD="RetweetedByMe"
  val CURRENT_USER_RETWEET_ID="CurrentUserRetweetId"
  val POSSIBLY_SENSITIVE_FIELD="PossiblySensitive"
  val LANG_FIELD="Lang"
  val WITHELD_COUNTRY_FIELD="WithheldInCountries"

  //user field
  val USER_FIELD="User"
  val NAME_FIELD="Name"
  val SCREEN_NAME_FIELD="ScreenName"
  val LOCATION_FIELD="Location"
  val DESCRIPTION_FIELD="Description"
  val CONTRIBUTORS_ENABLE_FIELD="ContributorsEnabled"
  val PROFILE_IMAGE_FIELD="OriginalProfileImageURL"
  val IS_DEFAULT_PROFILE_IMAGE_FIELD="DefaultProfileImage"
  val IS_PROTECTED_FIELD="Protected"
  val FOLLOWERS_COUNT_FIELD="FollowersCount"
  val IS_DEFAULT_BACKGROUND_FIELD="DefaultProfile"
  val FRIENDS_COUNT_FIELD="FriendsCount"
  val UTC_FIELD="UtcOffset"
  val TIME_ZONE_FIELD="TimeZone"
  val STATUS_COUNT_FIELD="StatusesCount"
  val IS_GEO_ENABLED="GeoEnabled"
  val IS_VERIFIED="Verified"

  //hashtag fields
  val HASHTAG_FIELD="HashtagEntities"

  //user mentions fields
  val USER_MENTION_FIELD="UserMentionEntities"
  val START_FIELD="Start"
  val END_FIELD="END"
  //media fields
  val MEDIA_FIELD="MediaEntities"

  //symbol fields
  val SYMBOL_FIELD="SymbolEntities"

  //URL Enties field
  val URL_ENTITIES_FIELD="URLEntities"


  //construct place schema
  val placeStruct = new StructType()
    .add(NAME_FIELD,DataTypes.StringType)
    .add(STREET_ADDRESS_FIELD,DataTypes.StringType)
    .add(COUNTRY_CODE_FIELD,DataTypes.StringType)
    .add(ID_FIELD,DataTypes.StringType)
    .add(COUNTRY_FIELD,DataTypes.StringType)
    .add(PLACE_TYPE_FIELD,DataTypes.StringType)
    .add(URL_FIELD,DataTypes.StringType)
    .add(FULL_NAME_FIELD,DataTypes.StringType)

  //construct geolocation schema
  val geoLocationStruct = new StructType()
    .add(LATITUDE_FIELD,DataTypes.FloatType)
    .add(LONGITUDE_FIELD,DataTypes.FloatType)

  //construct status deletion schema
  val statusDeletionStruct=new StructType()
    .add(STATUS_DELETION_FIELD,DataTypes.FloatType)
    .add(USER_DELETION_FIELD,DataTypes.FloatType)

  //construct user schema
  val userStruct=new StructType()
    .add(ID_FIELD,DataTypes.StringType)
    .add(NAME_FIELD,DataTypes.StringType)
    .add(SCREEN_NAME_FIELD,DataTypes.StringType)
    .add(LOCATION_FIELD,DataTypes.StringType)
    .add(DESCRIPTION_FIELD, DataTypes.StringType)
    .add(CONTRIBUTORS_ENABLE_FIELD, DataTypes.BooleanType)
    .add(PROFILE_IMAGE_FIELD, DataTypes.StringType)
    .add(IS_DEFAULT_PROFILE_IMAGE_FIELD, DataTypes.BooleanType)
    .add(URL_FIELD, DataTypes.StringType)
    .add(IS_PROTECTED_FIELD, DataTypes.BooleanType)
    .add(IS_DEFAULT_PROFILE_IMAGE_FIELD, DataTypes.BooleanType)
    .add(FOLLOWERS_COUNT_FIELD, DataTypes.IntegerType)
    .add(FRIENDS_COUNT_FIELD, DataTypes.IntegerType)
    .add(CREATED_AT_FIELD, DataTypes.TimestampType)
    .add(FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(UTC_FIELD, DataTypes.IntegerType)
    .add(TIME_ZONE_FIELD, DataTypes.StringType)
    .add(STATUS_COUNT_FIELD, DataTypes.IntegerType)
    .add(IS_GEO_ENABLED, DataTypes.BooleanType)
    .add(IS_VERIFIED, DataTypes.BooleanType)
    .add("Lang",DataTypes.StringType)

  val userMetionEntities=new StructType()
    .add(NAME_FIELD,DataTypes.StringType)
    .add(ID_FIELD,DataTypes.StringType)
    .add(TEXT_FIELD,DataTypes.StringType)
    .add(SCREEN_NAME_FIELD,DataTypes.StringType)

  val hashtagEntities=new StructType()
    .add(TEXT_FIELD,DataTypes.StringType)
    .add(START_FIELD,DataTypes.StringType)
    .add(END_FIELD,DataTypes.StringType)

  val statusStruct:StructType = new StructType()
    .add(CREATED_AT_FIELD,LongType)
    .add(ID_FIELD, DataTypes.StringType)
    .add(TEXT_FIELD, DataTypes.StringType)
    .add(SOURCE_FIELD, DataTypes.StringType)
    .add(TRUNCATED_FIELD, DataTypes.BooleanType)
    .add(IN_REPLY_TO_STATUS_ID, DataTypes.IntegerType)
    .add(IN_REPLY_TO_USER_ID, DataTypes.IntegerType)
    .add(IN_REPLY_TO_SCREEN_NAME, DataTypes.IntegerType)
    .add(GEO_LOCATION,geoLocationStruct)
    .add(PLACE_FIELD,placeStruct)
    .add(FAVORITED_FIELD, DataTypes.BooleanType)
    .add(RETWEETED_FIELD, DataTypes.BooleanType)
    .add(FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FIELD, userStruct)
    .add(RETWEET_FIELD,DataTypes.BooleanType)
    .add(CONTRIBUTORS_FIELD, DataTypes.createArrayType(DataTypes.IntegerType))
    .add(USER_MENTION_FIELD,DataTypes.createArrayType(userMetionEntities))
    .add(HASHTAG_FIELD, DataTypes.createArrayType(hashtagEntities))
    .add(RETWEET_COUNT_FIELD, DataTypes.IntegerType)
    .add(RETWEETED_BY_ME_FIELD, DataTypes.BooleanType)
    .add(CURRENT_USER_RETWEET_ID, DataTypes.IntegerType)
    .add(POSSIBLY_SENSITIVE_FIELD, DataTypes.BooleanType)
    .add(LANG_FIELD, DataTypes.StringType)
    .add(WITHELD_COUNTRY_FIELD, DataTypes.createArrayType(DataTypes.StringType))

  val payloadStruct:StructType= new StructType()
    .add("payload",statusStruct)


}
