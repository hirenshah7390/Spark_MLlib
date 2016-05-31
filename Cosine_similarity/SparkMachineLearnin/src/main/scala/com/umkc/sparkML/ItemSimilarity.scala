
import java.io.{PrintWriter, File}
import akka.io.Tcp.Write
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

//import org.bson.BSONObject
//import com.mongodb.hadoop.{
//MongoInputFormat, MongoOutputFormat,
//BSONFileInputFormat, BSONFileOutputFormat}
//import com.mongodb.hadoop.io.MongoUpdateWritable
//import org.apache.hadoop.conf.Configuration

/**created by Hiren Shah
  * A port of [[http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/]]
  * ref: [[http://www.grouplens.org/node/73]]
  */
object ItemSimilarity {

  def main(args: Array[String]) {
    /**
      * Parameters to regularize correlation.
      */
    //val mongoConfig = new Configuration()

    val PRIOR_COUNT = 10
    val PRIOR_CORRELATION = 0

    val TRAIN_FILENAME = "ml-100k//ua.base"
    //val TEST_FIELNAME = "ml-100k//ua.test"
    val food_FILENAME = "ml-100k//u.item"

    /**
      * Spark programs require a SparkContext to be initialized
      */

    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //mysql connection
    //    val prop = new java.util.Properties
    //    prop.setProperty("user","heartfit_root")
    //    prop.setProperty("password","heart123")
    //    val databaseConnectionUrl = "jdbc:mysql://srv70.hosting24.com:3306/heartfit_123"

    //mysql connection
    val databaseUsername = "heartfit_root"
    val databasePassword = "heart123"
    val databaseConnectionUrl = "jdbc:mysql://srv70.hosting24.com:3306/heartfit_123?user=" + databaseUsername + "&password=" + databasePassword

    val conf = new SparkConf()
      .setAppName("Food_nutrients")
      .set("spark.executor.memory", "2g").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //    mongoConfig.set("mongo.input.uri",
    //      "mongodb://root:test@ds021691.mlab.com:21691/food_nutrition.collection")
    //    //mongo.input.split.create_input_splits = false;
    //
    //    // Create an RDD backed by the MongoDB collection.
    //    val documents = sc.newAPIHadoopRDD(
    //      mongoConfig,                // Configuration
    //      classOf[MongoInputFormat],  // InputFormat
    //      classOf[Object],            // Key type
    //      classOf[BSONObject])        // Value type
    //
    //    //documents.foreach(d => println(d))
    //
    //    // Create a separate Configuration for saving data back to MongoDB.
    //    val outputConfig = new Configuration()
    //    outputConfig.set("mongo.output.uri",
    //      "mongodb://root:test@ds021691.mlab.com:21691/food_nutrition.collection")

    // Save this RDD as a Hadoop "file".
    // The path argument is unused; all documents will go to "mongo.output.uri".
    //    documents.saveAsNewAPIHadoopFile(
    //      "file:///this-is-completely-unused",
    //      classOf[Object],
    //      classOf[BSONObject],
    //      classOf[MongoOutputFormat[Object, BSONObject]],
    //      outputConfig)

    // get food names keyed on id
    val foods = sc.textFile(food_FILENAME)
      .map(line => {
        val fields = line.split("\\|")
        (fields(0).toInt, fields(1))
      })
    val foodNames = foods.collectAsMap()    // for local use to map id <-> foodname name for pretty-printing

    val matrix1 = sc.textFile(TRAIN_FILENAME)
      .map(line => {
        val fields = line.split("\t")
        (fields(0).toInt, fields(1).toInt, fields(2).toLong, fields(3).toLong,fields(4).toLong,fields(5).toLong,fields(6).toLong)
      })//.collect().filter(e => !(e._3 equals Double.NaN))

    //val matrix1 = matrix.collect().filter( f => !(f._1 > 10000) ).sortBy(e => e._1)

    // dummy copy of ratings for self join
    val matrix2 = matrix1.keyBy(tup => tup._1)

    //ratings2.foreach(r2 => println(r2))

    // join on userid and filter itmes pairs such that we don't double-count and exclude self-pairs
    val matrixPairs =
      matrix1
        .keyBy(tup => tup._1)
        .join(matrix2)
        .filter(f => (f._2._1._2 != f._2._2._2))

    //ratingPairs.foreach(rp => println(rp))

    // compute raw inputs to similarity metrics for each items pair
    val vectorCalcs =
      matrixPairs
        .map(data => {
          //println(data)
          val key = (data._2._1._2, data._2._2._2)
          val stats =
            ((data._2._1._3 * data._2._2._3)
              + (data._2._1._4 * data._2._2._4)
              + (data._2._1._5 * data._2._2._5)
              + (data._2._1._6 * data._2._2._6)
              + (data._2._1._7 * data._2._2._7),//dotproduct two matrix
              data._2._1._3,                 //not in use
              data._2._2._3,                //not in use
              (math.pow(data._2._1._3, 2) + math.pow(data._2._1._4, 2) + math.pow(data._2._1._5, 2) + math.pow(data._2._1._6, 2) + math.pow(data._2._1._7, 2)),   // square of
              (math.pow(data._2._2._3, 2) + math.pow(data._2._2._4, 2) + math.pow(data._2._2._5, 2) + math.pow(data._2._2._6, 2) + math.pow(data._2._2._7, 2)))   // square of
          // data._2._1._4,                //not in use
          // data._2._2._4)               //not in use
          //println(stats)
          (key, stats)
        })
        .groupByKey()
        .map(data => {
          val key = data._1
          val vals = data._2
          val size = vals.size
          val dotProduct =   (vals.map(f => f._1).sum)

          //println("dp " + dotProduct)

          val ratingSum = vals.map(f => f._2).sum   //not in use
          val rating2Sum = vals.map(f => f._3).sum  //not in use
          val ratingSq =  (vals.map(f => f._4).sum )
          //  println(ratingSq)
          val rating2Sq =  (vals.map(f => f._5).sum )
          //  println(rating2Sq)
          //  val numRaters = vals.map(f => f._6).max   //not in use
          //  val numRaters2 = vals.map(f => f._7).max  //not in use
          (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq))
        })

    // compute similarity metrics for each item pair
    val similarities =
      vectorCalcs
        .map(fields => {
          val key = fields._1
          val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq) = fields._2
          //val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
          //val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum,
          //  ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)

          val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
          // val jaccard = jaccardSimilarity(size, numRaters, numRaters2)

          (key, (cosSim))
        })

    //similarities.foreach(s => println(s))

    // test a few items out (substitute the contains call with the relevant item name
    val sample = similarities.filter(m => {
      val items = m._1
      (foodNames(items._1).contains("Butter, salted"))
    })

    // collect results, excluding NaNs if applicable
    val result = similarities.map(v => {
      val m1 = v._1._1
      val m2 = v._1._2
      // val corr = v._2._1
      // val rcorr = v._2._2
      val cos = v._2
      // val j = v._2._4
      (m1,m2, cos)
    }).collect().filter(e => !(e._3 equals Double.NaN))// test for NaNs must use equals rather than ==
      .sortBy(elem => elem._3)
      .sortBy(elem => elem._1)
      .reverse//.take(10)

    //result.foreach(println)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    val writer = new PrintWriter(new File("output.txt"))
    result.foreach(r => writer.write({if(r._1 < 10000) ("0") else ("")} + r._1 + "," + {if(r._2 < 10000) ("0") else ("")} + r._2 + "," +  r._3.formatted("%2.4f") + "\r\n" ))
    writer.close()

    val df = sc.textFile("output.txt").map(m =>m.split(",")).map(p => Items(p(0),p(1),p(2).toDouble)).toDF()

    val items = Window.partitionBy('I1).orderBy('csm.desc)
    //val rank = row_number().over(items)
    val ranked = df.withColumn("rank", row_number().over(items))
    ranked.registerTempTable("resultitems")

    val finalresults = sqlContext.sql("SELECT * FROM resultitems where rank < 6")

    finalresults.insertIntoJDBC(databaseConnectionUrl,"Similarity",false)
    //finalresults.write.jdbc(databaseConnectionUrl,"Similarity",prop) //(databaseConnectionUrl, "languagetweets", true)

  }

  case class Items(I1: String,I2: String, csm: Double)

  // *************************
  // * SIMILARITY MEASURES
  // *************************

  /**
    * The correlation between two vectors A, B is
    *   cov(A, B) / (stdDev(A) * stdDev(B))
    *
    * This is equivalent to
    *   [n * dotProduct(A, B) - sum(A) * sum(B)] /
    *     sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
    */
  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
                  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
  }

  /**
    * Regularize correlation by adding virtual pseudocounts over a prior:
    *   RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
    * where w = # actualPairs / (# actualPairs + # virtualPairs).
    */
  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double,
                             rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
                             virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
    * The cosine similarity between two vectors A, B is
    *   dotProduct(A, B) / (norm(A) * norm(B))
    */
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
    * The Jaccard Similarity between two sets A, B is
    *   |Intersection(A, B)| / |Union(A, B)|
    */
  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}