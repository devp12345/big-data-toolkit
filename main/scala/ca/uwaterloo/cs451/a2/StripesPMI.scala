
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.collection.mutable


object StripesPMI extends Tokenizer {
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

    val args = new PMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("Stripes PMI Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val keyPartitioner = new KeyPartitioner(args.reducers())
    val threshold = args.threshold()
    val partitions = args.reducers()

    val accumulator = sc.longAccumulator
    val textFile = sc.textFile(args.input())

    val frequencyMap = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        accumulator.add(1)
        if (tokens.length > 0) tokens.map(token => (token, 1)) else List()
      })
      .reduceByKey(keyPartitioner, _ + _)
      .collectAsMap()

    
    val lineCount = sc.broadcast(accumulator.value)
    val frequencies = sc.broadcast(frequencyMap)

    // go thru each line take first 40 distinct
    // make stripes for each word
    // put the stripes into bigger stripes buffer, 
    // merge together, follow jimmy lins logic, handle empty in s1 but item in s2
    // filter out k,v with v < threshold
    // filter empty maps as a result of threshold
    // build pmi data
    // return (pair, (pmi, freq))

    val PMI_DATA = textFile
        .flatMap(line => {
            val tokens = tokenize(line).take(40).distinct
            if(tokens.length > 1) {
                tokens.map( token1 => {
                    val stripe = mutable.Map[String, Int]()
                    tokens
                        .filter(_ != token1)
                        .foreach(token2 => stripe(token2) = 1)

                    val immutableStripe = stripe.toMap
                    (token1, immutableStripe)
                })
            } else {
                List()
            }
        })
        .reduceByKey(keyPartitioner, (stripe1, stripe2) => mergeStripes(stripe1, stripe2))
        .map(key_stripe => {
            val key = key_stripe._1
            val unfilteredStripe = key_stripe._2
            val stripe = unfilteredStripe.filter(_._2 >= threshold)
            (key, stripe)
        })
        .filter(item => !item._2.isEmpty)
        .map(keyAndStripe => {
            val key = keyAndStripe._1
            val stripe = keyAndStripe._2
            val pmiMap = mutable.Map[String, (Double, Int)]()

            val left = key
            val leftCount = frequencies.value(left)
            val lines = lineCount.value.toFloat

            for ((right, count) <- stripe) {
                val rightCount = frequencies.value(right)
                val pmi = Math.log10((count * lines) / (leftCount * rightCount))

                pmiMap(right) = (pmi, count)
            }

            (key, pmiMap.toMap)
            }
        )

      PMI_DATA.saveAsTextFile(args.output())

      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d
      log.info(s"Stripes PMI finished in: $duration seconds")


  }
}
