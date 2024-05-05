
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.collection.mutable

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = Math.abs(key.hashCode) % numPartitions
  }

    def mergeStripes(stripe1: Map[String, Int], stripe2: Map[String, Int]): Map[String, Int] = {
        var stripe = stripe1
            for ((k, v) <- stripe2) {
                stripe = stripe.updated(k, v + stripe.getOrElse(k, 0))
            }
        stripe
    }

  def main(argv: Array[String]) {

    val args = new BigRamConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("Stripes Bigram Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val keyPartitioner = new KeyPartitioner(args.reducers())
    val textFile = sc.textFile(args.input())

    // go thru each line
    // add token2 to token1 map to rep token1 comes before token2
    // accumulate the stripes w prtitioner and mergeStripes func
    // find the overal freq/sum of it
    // do math
    // map output

    val BIGRAMS = textFile
    .flatMap(line => {
      val tokens = tokenize(line)
      if(tokens.length > 1){
        tokens.sliding(2).map(pair => {
            (pair(0), Map(pair(1) -> 1))
        }).toList
      } else{
        List()
      }
    })
    .reduceByKey(keyPartitioner, (stripe1, stripe2) => mergeStripes(stripe1, stripe2))
    .map(keyAndStripe => {
      val key = keyAndStripe._1
      val stripe = keyAndStripe._2
      val bigramMap = mutable.Map[String, Double]()

      var freq = 0
      for((_, v) <- stripe){
        freq += v
      }

      for((k, v) <- stripe){
        val bigram = v.toDouble / freq.toDouble
        bigramMap(k) = bigram
      }

      (key, bigramMap.toMap)
    })
    
    BIGRAMS.saveAsTextFile(args.output())
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"Stripes Bigram finished in: $duration seconds")

  }
}
