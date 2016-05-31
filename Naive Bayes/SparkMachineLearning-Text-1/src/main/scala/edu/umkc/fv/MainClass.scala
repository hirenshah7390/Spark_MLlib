package edu.umkc.fv

/**
 * Created by Mayanka on 23-Jul-15.
 */

import edu.umkc.fv.NLPUtils._
import edu.umkc.fv.Utils._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import edu.umkc.fv.FeatureVector1
import scala.io.Source

object MainClass {

  def main(args: Array[String]) {

    val filters = Source.fromFile("./filter/allfilter.txt").getLines.toArray

    var filename = 61258
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
    val sc = ssc.sparkContext
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    //stream.print()

    val text = stream.map(tweet =>(tweet.getId,tweet.getText()))

    val stopWords = sc.broadcast(loadStopWords("/stopwords.txt")).value

    val labelToNumeric = createLabelMap("data/training/")
    labelToNumeric.foreach( l => println(l))
    var model: NaiveBayesModel = null
    // Training the data
    val training = sc.wholeTextFiles("data/training/*")
      .map(rawText => createLabeledDocument(rawText, labelToNumeric, stopWords))
    val X_train = tfidfTransformer(training)
    // X_train.foreach(vv => println(vv))

    model = NaiveBayes.train(X_train, lambda = 1.0)

    val lines=sc.wholeTextFiles("data/testing/*")
    val data = lines.map(line => {
      val test = createLabeledDocumentTest(line._2, labelToNumeric, stopWords)
      //println(test.body)
      test
    })

    val X_test = tfidfTransformerTest(sc, data)

    val predictionAndLabel = model.predict(X_test)
    println("PREDICTION")
    predictionAndLabel.foreach(x => {
      labelToNumeric.foreach { y => if (y._2 == x) {
        println(y._1)
      }
      }
    })


    text.foreachRDD(rdd => {
      //val tweets = rdd
    //  rdd.foreach(println)
      rdd.foreach{case(id,string) =>
        // val sentimentAnalyzer: SentimentAnalyzer = new SentimentAnalyzer
        // val tweetWithSentiment: TweetWithSentiment = sentimentAnalyzer.findSentiment(string)
         val ht =  string.split(" ")
         var hashtags = ""
         for(i <- 0 to ht.length-1){
            for(j  <- 0 to filters.length-1)
             {
               if(filters(j).toLowerCase() == ht(i).toLowerCase() && hashtags != "" )
                 hashtags = hashtags + "|" + filters(j)
               else if (filters(j).toLowerCase() == ht(i).toLowerCase())
                 hashtags = filters(j)

               //System.out.println(ht(i) + " " + filters(j))
             }
         }
        val finalstring = id.toString() + "::" +string + "::"+hashtags.replace("#","")
       // System.out.println(finalstring)
       // scala.tools.nsc.io.File("./Filter/Trainingdata.txt").appendAll(finalstring + "\r\n")
        //SocketClient.sendCommandToRobot(tweetWithSentiment.toString)
        scala.tools.nsc.io.File("./data/testing/test/"+ filename+".txt").appendAll(finalstring)
          filename+=1

      }

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
