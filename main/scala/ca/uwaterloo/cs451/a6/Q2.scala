
package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new A6Conf(argv)

    if(!args.text() && !args.parquet()) {
      log.error("ERROR: no valid format presented")
      return
    }

    val formatType = if (args.text()) "text" else "parquet"
    val shipdate = args.date()
    log.info("Input: " + args.input())
    log.info("Date: " + shipdate)
    log.info("Format type: " + formatType)

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("Q2 query")
    val sc = new SparkContext(conf)

    var lineItemOnDate: Option[RDD[(Int, String)]] = None
    var orders: Option[RDD[(Int, String)]] = None

    if(args.text()){ // text

        val lineItem = sc.textFile(args.input() + "/lineitem.tbl")
        val ordersRDD = sc.textFile(args.input() + "/orders.tbl")

        lineItemOnDate = Some(lineItem.filter(line => {
            val tokens = line.split("\\|")
            tokens(10) == (shipdate)
        }).map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(10).toString)
        }))

        orders = Some(ordersRDD.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(6).toString)
        }))
    } 
    else { // parquet

        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd

        lineItemOnDate = Some(lineitemRDD.filter(line => {
            line(10) == (shipdate)
        }).map(line => {
            (line.getAs[Int](0), line(10).toString)
        }))

        orders = Some(ordersRDD.map(line => {
            (line.getAs[Int](0), line(6).toString)
        }))
    }

    (lineItemOnDate, orders) match {
        case (Some(lItemDate), Some(ord)) =>
            val top20 = lItemDate.cogroup(ord)
                .flatMap {
                    case (orderKey, (shipDates, clerks)) =>
                        if (shipDates.nonEmpty && clerks.nonEmpty) {
                            Some((clerks.iterator.next().toString, orderKey))
                        } else {
                            None
                        }
                }
                .sortBy(_._2)
                .take(20).foreach(println)
        case _ => println("One of the RDDs is not initialized")
    }
    
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"Q2 finished in: $duration seconds")

  }
}
