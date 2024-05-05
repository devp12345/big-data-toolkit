
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class PMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
    def getPartition(key: Any): Int = Math.abs(key.hashCode) % numPartitions
  }

  class PairPartitioner(override val numPartitions: Int) extends Partitioner {
     def getPartition(key: Any): Int = key match {
      case (s1: String, s2: String) => {
        Math.abs(s1.hashCode) % numPartitions
      }
     }
  }


  def main(argv: Array[String]) {

    val args = new PMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

        val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("Pairs PMI Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val keyPartitioner = new KeyPartitioner(args.reducers())
    val pairPartitioner = new PairPartitioner(args.reducers())

    val threshold = args.threshold()

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
    // make pairs
    // accumulate the pairs w prtitioner
    // check if greater than threshold
    // build pmi data
    // return (pair, (pmi, freq))

    val PMI_DATA = textFile
    .flatMap(line => {
      val tokens = tokenize(line).take(40).distinct
      if(tokens.length > 1){
        tokens.flatMap(token1 =>{
          tokens.filter(_ != token1).map(token2 => ((token1, token2), 1))
        })
      }else{
        List()
      }
    })
    .reduceByKey(pairPartitioner, _ + _)
    .filter(item => item._2 >= threshold).map(pairAndCount => {
          val pair = pairAndCount._1
          val count = pairAndCount._2

          val left = pair._1
          val right = pair._2

          val leftCount = frequencies.value(left)
          val rightCount = frequencies.value(right)
          val lines = lineCount.value.toFloat

          val pmi = Math.log10((count * lines) / (leftCount * rightCount))
          (pair, (pmi, count))
        }
      )

      PMI_DATA.saveAsTextFile(args.output())

      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d
      log.info(s"Pairs PMI finished in: $duration seconds")

  }
}
