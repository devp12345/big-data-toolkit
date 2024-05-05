
package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}

object Q1 extends Tokenizer {
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

    val conf = new SparkConf().setAppName("Q1 query")
    val sc = new SparkContext(conf)

    if(args.text()){ // text

      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      val count = textFile.filter(line => {
        val tokens = line.split("\\|")
        tokens(10) == (shipdate)
      }).count()

      println("ANSWER=" + count)

    } else { // parquet

      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val count = lineitemRDD.filter(line => line(10) == (shipdate)).count()

      println("ANSWER=" + count)

    }

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"Q1 finished in: $duration seconds")

  }
}
