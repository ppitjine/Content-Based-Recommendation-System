import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.List


object Weinung_Chao_task2 {
  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    val sparkConf = new SparkConf().setAppName("Weinung_Chao_task2").setMaster("local[*]").set("spark.executor.memory", "4g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
      num.toDouble( ts.sum ) / ts.size
    }

    val games = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))
//    val games = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("ratings.csv")

    //    (userid,movieid,rating)
    var ratingspair = games.rdd.map(row =>
      (row.getInt(0), row.getInt(1), row.getDouble(2))
    )

    val games2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))
//    val games2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("testing_small.csv")

    //    (userid,movieid)
    var usersProducts =games2.rdd.map(row =>
      (row.getInt(0), row.getInt(1))
    )


    //(userid,Iterable(userid,movieid,rating) )
    val usertomovies = ratingspair.groupBy(k => k._1)

    //rater size per movie
//    (raterid, size)
    val moviesize_peruser = usertomovies.map(k => (k._1,k._2.size))


    //(userid,movieid,rating,size)
    val ratingpairwithsize = usertomovies.join(moviesize_peruser).flatMap{k=>
      k._2._1.map(u=> (u._1,u._2,u._3,k._2._2))
    }

    //for join on movieid
    val ratings2 = ratingpairwithsize.keyBy(tup => tup._2)
    //    (movieid,((userid,movieid,rating,size),(userid,movieid,rating,size)))
    val tworatings =ratings2.join(ratings2).filter(f => f._2._1._1 < f._2._2._1)


    val user_base_cf =
      tworatings.map(data => {
        ((data._2._1._1, data._2._2._1),
          (data._2._1._3 * data._2._2._3,data._2._1._3,data._2._2._3,
            math.pow(data._2._1._3, 2),
            math.pow(data._2._2._3, 2),
            data._2._1._4,
            data._2._2._4))
      }).groupByKey().map(data => {
        (data._1,
          (data._2.size,
            data._2.map(f => f._1).sum,
            data._2.map(f => f._2).sum,
            data._2.map(f => f._3).sum,
            data._2.map(f => f._4).sum,
            data._2.map(f => f._5).sum,
            data._2.map(f => f._6).max,
            data._2.map(f => f._7).max))
      })

    val sims =
      user_base_cf.map(fields => {
        val cos = fields._2._2/(scala.math.sqrt(fields._2._5) * scala.math.sqrt(fields._2._6))
        (fields._1._1,(fields._1._2, cos))
      })
    //(userid(userid, sim))


    val invSim = sims.map(ori=>(ori._2._1,(ori._1,ori._2._2)))
    val simTotal =  sims ++ invSim

    // for add rate
    // (userid,(movieid,(userid,sim))) => ((cal_userid,moiveid),(predict_userid,sim))
    val simTotal_join =  usersProducts.join(simTotal).map(u=> ((u._2._2._1,u._2._1),(u._1,u._2._2._2)))

    //((userid,movieid),rating)
    val ratingspair2 =ratingspair.map(x => ((x._1,x._2),x._3))
    //(movieid,(movieid,sim,rating))
    //((cal_userid,movieid),((predict_userid,sim),rating)) => ((userid,movieid),(sim,rating))
    val simTotal_with_rating = simTotal_join.join(ratingspair2).map(x => ((x._2._1._1,x._1._2),(x._2._1._2,x._2._2)))
    //(movieid,List(movieid,sim,rating))

//    ((userid,movieid),(sim.rating))
    val cutNumSim= simTotal_with_rating.groupByKey().map(sim=>(sim._1,sim._2.toList.sortBy(x=>x._1).reverse.take(10)))



    val accu = cutNumSim.map{
      u=>
        var div = u._2.map(f=> (f._1*f._2,f._1)).reduce((x,y)=> (x._1+y._1,x._2+y._2))
        (u._1,div)
    }

    var predictions = accu.map{x=>
      var pre = x._2._1/x._2._2
      if(pre >5)
        pre =5
      if(pre<0)
        pre =0
      ((x._1._1, x._1._2), pre)}


    var for_loose_data = usersProducts.map(x => (x,Double.NaN))
    for_loose_data = predictions ++ for_loose_data
    var pp =for_loose_data.groupByKey().map{x =>
      var es = x._2.toList
      var rating = Double.NaN
      if(es.size ==2)
        rating = es.head
      (x._1,rating)
    }

    val mean = ratingspair.map(x=> (x._2,x._3)).groupByKey().map(x=>(x._1,average(x._2))).sortBy(x =>x._1).collect().toList


    pp =pp.map{ x=>
      var rating = x._2
      if(rating.equals(Double.NaN))
        rating = mean.find(p =>p._1 ==x._1._2).head._2
      (x._1,rating)
    }
    predictions =pp


    val ratesAndPreds = ratingspair.map { case (user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    var RMSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    RMSE = math.sqrt(RMSE)

    var result = predictions.sortBy(line => (line._1._1, line._1._2), ascending=true).collect()

    val file = "Weinung_Chao_result_task2.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    writer.write("UserId,MovieId,Pred_rating\n")
    for (x <- result) {
      writer.write(x._1._1+","+x._1._2+","+x._2+"\n")
      //      writer.write(x+"\n")
    }

    var tmp1 = predictions.filter(x => x._2>=0 &&x._2<1).count()
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
