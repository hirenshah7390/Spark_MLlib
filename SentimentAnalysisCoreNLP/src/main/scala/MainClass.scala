
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainClass {

  def main(args: Array[String]) {

    val filters = Array("oscar","leo","Primary","election","clinton","cruz","trump","sanders");
   // val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "YDvMtLCSLnslXX3m4pY4z2aDb")
    System.setProperty("twitter4j.oauth.consumerSecret", "cOi1FkNVgibru1rp1Rtcy3sdUjzujc5JAGowxk0YkFmbq0Otmu")
    System.setProperty("twitter4j.oauth.accessToken", "3534109580-EnBKBt17Y2XEuaZc8cmUXPLNRgIfZN1vM0NBB6w")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "UavesZwDDMEDmkWgutZXGvBDynE4qB4QxktiuHbIKtw0W")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    //stream.print()

    val text = stream.map(tweet => tweet.getText())

    text.foreachRDD(rdd => {
      //val tweets = rdd
    //  rdd.foreach(println)
      rdd.foreach{string =>
         val sentimentAnalyzer: SentimentAnalyzer = new SentimentAnalyzer
         val tweetWithSentiment: TweetWithSentiment = sentimentAnalyzer.findSentiment(string)
         //System.out.println(tweetWithSentiment)
        SocketClient.sendCommandToRobot(tweetWithSentiment.toString)
      }

    })
    ssc.start()

    ssc.awaitTermination()
  }
}
