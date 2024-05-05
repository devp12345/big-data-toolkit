
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class BigRamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  class PairPartitioner(override val numPartitions: Int) extends Partitioner {
     def getPartition(key: Any): Int = key match {
      case (s1: String, s2: String) => {
        Math.abs(s1.hashCode) % numPartitions
      }
     }
  }

  def main(argv: Array[String]) {

    val args = new BigRamConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("Pairs Bigram Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val pairPartitioner = new PairPartitioner(args.reducers())
    val textFile = sc.textFile(args.input())

    // go thru each line
    // make pairs (token1, token2) to rep token1 comes before token2
    // also make (token1, "*") to rep it comes before a word in general
    // "I am a student" -> ((I, am), 1) ((I, *), 1) ... ((a, student), 1) ((a, *), 1), no student
    // accumulate the pairs w prtitioner
    // group all by keys into a list (a, [(b, 3), (c, 5)])
    // find * in the list, get its value
    // do math
    // flatmap list

    val BIGRAMS = textFile
    .flatMap(line => {
      val tokens = tokenize(line)
      if(tokens.length > 1){
        tokens.sliding(2).flatMap(pair => {
          List(((pair(0), pair(1)), 1), ((pair(0), "*"), 1))
        })
      } else{
        List()
      }
    })
    .reduceByKey(pairPartitioner, _ + _)
    .map(pairAndCount => {
      val key = pairAndCount._1._1
      val value = pairAndCount._1._2
      val count = pairAndCount._2
      (key, (value, count))
    })
    .groupByKey()
    .flatMap(keyGrouping => {           // (a, [(b, 3), (c, 5)])

      // find * in the list, get its value
      // do math
      // map or flatmap out the strings - have to do it manually 
      // ((dream,*),13743.0), 
      // ((dream,a),0.013388635)
      // ...

      val key = keyGrouping._1
      val values = keyGrouping._2

      val optionalMarginal: Option[Float] = values.find(_._1 == "*").map(_._2)
      val marginal: Float = optionalMarginal.getOrElse(0.0f)

      if (marginal == 0) {
        log.info(s"GOT 0 AS MARGINAL VALUE FOR KEY: $key")
        List()
      } else {
        values.map {
          case (token, freq) => 
            if(token == "*"){
              s"(($key,$token),$freq)"
            }
            else{
              val relativeFrequency: Float = freq / marginal
              s"(($key,$token),$relativeFrequency)"
            }
        }
      }
    })
    
      BIGRAMS.saveAsTextFile(args.output())

      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d
      log.info(s"Pairs Bigram finished in: $duration seconds")

  }
}
