
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


object Q3 extends Tokenizer {
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

    var partsMap: Option[Map[Int, String]] = None
    var suppMap: Option[Map[Int, String]] = None
    var lineItemsRDD: Option[RDD[(Int, Int, Int, String)]] = None

    if(args.text()){ // text

        val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        val partsFile = sc.textFile(args.input() + "/part.tbl")
        val suppliersFile = sc.textFile(args.input() + "/supplier.tbl")

        partsMap = Some(partsFile.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(1))
        }).collectAsMap().toMap)

        suppMap = Some(suppliersFile.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(1))
        }).collectAsMap().toMap)

        lineItemsRDD = Some(lineItems.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(10).toString)
        }))
    } 
    else { // parquet

        val sparkSession = SparkSession.builder.getOrCreate

        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd

        val partDF = sparkSession.read.parquet(args.input() + "/part")
        val partRDD = partDF.rdd

        val suppDF = sparkSession.read.parquet(args.input() + "/supplier")
        val suppRDD = suppDF.rdd

        partsMap = Some(partRDD.map(line => {
            (line.getAs[Int](0), line(1).toString)
        }).collectAsMap().toMap)

        suppMap = Some(suppRDD.map(line => {
            (line.getAs[Int](0), line(1).toString)
        }).collectAsMap().toMap)

        lineItemsRDD = Some(lineitemRDD.map(line => {
            (line.getAs[Int](0), line.getAs[Int](1), line.getAs[Int](2), line(10).toString)
        }))
    }

    (partsMap, suppMap, lineItemsRDD) match {
        case (Some(partsMap), Some(suppMap), Some(lineItemsRDD)) =>
            val parts = sc.broadcast(partsMap)
            val suppliers = sc.broadcast(suppMap)

            lineItemsRDD
                .filter(item => {item._4 == shipdate && parts.value.contains(item._2) && suppliers.value.contains(item._3)})
                .map(item => (item._1, (item._1, parts.value(item._2), suppliers.value(item._3))))
                .sortByKey()
                .take(20)
                .map(item => item._2)
                .foreach(println)
        case _ => println("Data not loaded properly")
    }
    
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"Q3 finished in: $duration seconds")

  }
}
