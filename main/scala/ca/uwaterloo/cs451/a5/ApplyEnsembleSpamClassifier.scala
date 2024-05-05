
package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.math.exp

class ApplyEnsembleSpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new ApplyEnsembleSpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier Spark")
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

    val xWeights = getWeightsFromModels(args.model() + "/part-00000")
    val yWeights = getWeightsFromModels(args.model() + "/part-00001")
    val britneyWeights = getWeightsFromModels(args.model() + "/part-00002")

    val xBroadcast = sc.broadcast(xWeights)
    val yBroadcast = sc.broadcast(yWeights)
    val britneyBroadcast = sc.broadcast(britneyWeights)

    val textFile = sc.textFile(args.input(), 1)

    val ensembleMethod = args.method()

    val PREDICTIONS = textFile.map(line => {
        val lineSplit = line.split(" ")

        val docid = lineSplit(0)
        val testLabel = lineSplit(1)
        val features = lineSplit.drop(2).map(feature => feature.toInt)

        val xSpaminess = spamminess(features, xBroadcast.value)
        val ySpaminess = spamminess(features, yBroadcast.value)
        val britneySpaminess = spamminess(features, britneyBroadcast.value)

        if (ensembleMethod == "average") {
            val score = (xSpaminess + ySpaminess + britneySpaminess) / 3
            val predictedLabel = if (score > 0) "spam" else "ham"
            (docid, testLabel, score, predictedLabel)
        } else if (ensembleMethod == "vote") {
            val xVote = if (xSpaminess > 0) 1d else -1d 
            val yVote = if (ySpaminess > 0) 1d else -1d 
            val britneyVote = if (britneySpaminess > 0) 1d else -1d 
            val score = xVote + yVote + britneyVote
            val predictedLabel = if (score > 0) "spam" else "ham"
            (docid, testLabel, score, predictedLabel)
        } else {
            log.info("NOT A VALID VOTE TYPE " + ensembleMethod)
        }
    })

    PREDICTIONS.saveAsTextFile(args.output())

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"ApplyEnsembleSpamClassifier finished in: $duration seconds")

  }
}
