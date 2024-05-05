
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


object Q4 extends Tokenizer {
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

    var nationMap: Option[Map[Int, String]] = None
    var customerMap: Option[Map[Int, Int]] = None
    var lineItemOnDate: Option[RDD[(Int, String)]] = None
    var orders: Option[RDD[(Int, Int)]] = None

    if(args.text()){ // text
        val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        val ordersFile = sc.textFile(args.input() + "/orders.tbl")
        val nationsFile = sc.textFile(args.input() + "/nation.tbl")
        val customersFile = sc.textFile(args.input() + "/customer.tbl")

        nationMap = Some(nationsFile.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(1)) // nationKey, name
        }).collectAsMap().toMap)

        customerMap = Some(customersFile.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(3).toInt) // customerKey, nationKey
        }).collectAsMap().toMap)


        lineItemOnDate = Some(lineItems.filter(line => {
            val tokens = line.split("\\|")
            tokens(10) == (shipdate)
        }).map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(10).toString) // orderKey, shipdate
        }))

        orders = Some(ordersFile.map(line => {
            val tokens = line.split("\\|")
            (tokens(0).toInt, tokens(1).toInt) // orderKey, customerKey
        }))
    } 
    else {
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd
        val customerDF = sparkSession.read.parquet(args.input() + "/customer")
        val customerRDD = customerDF.rdd
        val nationDF = sparkSession.read.parquet(args.input() + "/nation")
        val nationRDD = nationDF.rdd

        nationMap = Some(nationRDD.map(tokens => {
            (tokens.getAs[Int](0), tokens.getAs[String](1)) // nationKey, name
        }).collectAsMap().toMap)

        customerMap = Some(customerRDD.map(tokens => {
            (tokens.getAs[Int](0), tokens.getAs[Int](3)) // customerKey, nationKey
        }).collectAsMap().toMap)

        lineItemOnDate = Some(lineitemRDD.filter(tokens => {
            tokens(10) == (shipdate)
        }).map(tokens => {
            (tokens.getAs[Int](0), tokens.getAs[String](10)) // orderKey, shipdate
        }))

        orders = Some(ordersRDD.map(tokens => {
            (tokens.getAs[Int](0), tokens.getAs[Int](1)) // orderKey, customerKey
        }))

    }

    (customerMap, nationMap, lineItemOnDate, orders) match {
        case (Some(customerMap), Some(nationMap), Some(lineItemOnDate), Some(orders)) => 
            val customerNationMap = customerMap.flatMap {
            case (custkey, nationKey) => 
                nationMap.get(nationKey).map(nationName => (custkey, (nationKey, nationName))) // custKey -> (nationKey, name)
            }.toMap

            val customerNationBroadcast = sc.broadcast(customerNationMap)

            val ordersOnDate = orders.cogroup(lineItemOnDate)
                    .filter(pair => pair._2._2.iterator.hasNext) 
            
            // go through the orders (orderKey, (customerKey, shipdate)) from cogroup
            ordersOnDate.flatMap(orders => 
                // lineItems, customerKey is Iterable, flatMap both to make sure we count all items. 
                // orderKey maps to many items
                orders._2._2.flatMap(lineItem =>
                    orders._2._1.flatMap(custKey => 
                    // get the value from map (nationKey, name) to make tuples (nationKey, 1)
                        customerNationBroadcast.value.get(custKey).map(nation => (nation._1, 1))
                    )
                )
            )
            .reduceByKey(_ + _) // count all by Key
            .collect()
            .map(nationCount => 
                (nationCount._1, nationMap.get(nationCount._1), nationCount._2)
            )
            .map{
                case (key, Some(name), count) => (key, name, count)
            } // remove Some
            .sortBy(_._1)
            .foreach(println)

        case _ => println("Data not loaded properly")
    }
    
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"Q4 finished in: $duration seconds")

  }
}
