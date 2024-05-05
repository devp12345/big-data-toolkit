
package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.math.exp

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new ApplySpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("ApplySpamClassifier Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (weights.contains(f)) score += weights(f))
        score
    }

    def getWeightsFromModels(modelPath : String) : scala.collection.Map[Int, Double] = {
        val modelFile = sc.textFile(modelPath)
        val w = modelFile.map(modelFeature => {
            val stripped = modelFeature.stripPrefix("(").stripSuffix(")")
            val parts = stripped.split(",")
            val feature = parts(0).trim.toInt
            val weight = parts(1).trim.toDouble
            (feature, weight)
        }).collectAsMap()
        w
    }

    val w = getWeightsFromModels(args.model() + "/part-00000")
    val wBroadcast = sc.broadcast(w)

    val textFile = sc.textFile(args.input(), 1)

    val PREDICTIONS = textFile.map(line => {
        val lineSplit = line.split(" ")

        val docid = lineSplit(0)
        val testLabel = lineSplit(1)
        val features = lineSplit.drop(2).map(feature => feature.toInt)
        
        val score = spamminess(features, wBroadcast.value)
        val predictedLabel = if (score > 0) "spam" else "ham"
        (docid, testLabel, score, predictedLabel)
    })

    PREDICTIONS.saveAsTextFile(args.output())

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"ApplySpamClassifier finished in: $duration seconds")

  }
}
