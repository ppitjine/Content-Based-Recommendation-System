import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Weinung_Chao_task1 {
  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    val sparkConf = new SparkConf().setAppName("Weinung_Chao_task1").setMaster("local[*]").set("spark.executor.memory", "4g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val games = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))
    var ratingspair = games.rdd.map(row =>
      Rating(row.getInt(0), row.getInt(1), row.getDouble(2))
    )

    def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
      num.toDouble( ts.sum ) / ts.size
    }



//    val games = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/Users/chaoweinung/Documents/try/ratings.csv")

    val games2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

//    val games2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("testing_small.csv")


    var usersProducts =games2.rdd.map(row =>
      (row.getInt(0), row.getInt(1))
    )



    var list_usersProducts = usersProducts.collect()
    var new_ratingspair =ratingspair.filter(f =>  !list_usersProducts.contains((f.user,f.product)))


    // Build the recommendation model using ALS
    val rank = 8
    val numIterations = 10
    val model = ALS.train(new_ratingspair, rank, numIterations, 0.01)

    var predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    predictions = predictions.map{x=>
      var pre = x._2
      if(pre >5)
        pre =5
      if(pre<0)
        pre =0

      ((x._1._1, x._1._2), pre)
    }

    var for_loose_data = usersProducts.map(x => (x,Double.NaN))
    for_loose_data = predictions ++ for_loose_data
    var pp =for_loose_data.groupByKey().map{x =>
      var es = x._2.toList
      var rating = Double.NaN
      if(es.size ==2)
        rating = es.head
      (x._1,rating)
    }

    val mean = ratingspair.map(x=> (x.product,x.rating)).groupByKey().map(x=>(x._1,average(x._2))).sortBy(x =>x._1).collect().toList

    pp =pp.map{ x=>
      var rating = x._2
      if(rating.equals(Double.NaN))
        rating = mean.find(p =>p._1 ==x._1._2).head._2
      (x._1,rating)
    }
    predictions =pp


    val ratesAndPreds = ratingspair.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    var RMSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    RMSE = math.sqrt(RMSE)
    val file = "Weinung_Chao_result_task1.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))


    writer.write("UserId,MovieId,Pred_rating\n")


    var result = predictions.sortBy(line => (line._1._1, line._1._2), ascending=true).collect()
    for (x <- result) {
        writer.write(x._1._1+","+x._1._2+","+x._2+"\n")
    }
    var tmp1 = predictions.filter(x => x._2<1).count()
    var tmp2 = predictions.filter(x => x._2>=1 &&x._2<2).count()
    var tmp3 = predictions.filter(x => x._2>=2 &&x._2<3).count()
    var tmp4 = predictions.filter(x => x._2>=3 &&x._2<4).count()
    var tmp5 = predictions.filter(x => x._2>=4).count()

    writer.close()
    sc.stop()

    println(">=0 and <1: " +tmp1)
    println(">=1 and <2: " +tmp2)
    println(">=2 and <3: " +tmp3)
    println(">=3 and <4: " +tmp4)
    println(">=4: "        +tmp5)
    println("RMSE: "+RMSE)
    val t1 = System.nanoTime()
    println("The total execution time taken is" + (t1 - t0)/1000000000.0000 +"sec." )

  }

}
