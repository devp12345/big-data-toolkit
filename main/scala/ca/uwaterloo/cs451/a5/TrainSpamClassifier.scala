
package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.math.exp
import scala.util.Random

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "do shuffle", required = false)
  verify()
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new TrainSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val startTime = System.nanoTime()

    val conf = new SparkConf().setAppName("TrainSpamClassifier Spark")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var textFile = sc.textFile(args.input(), 1)
    if(args.shuffle()){
      textFile = textFile.map(textLine => (Random.nextInt(), textLine)).sortByKey().map(_._2)
    }

    // w is the weight vector (make sure the variable is within scope)
    val w = scala.collection.mutable.Map[Int, Double]() // (feature int, weight double)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    // This is the main learner:
    val delta = 0.002

    val TRAINED = textFile.map(line => {
        val lineSplit = line.split(" ")
        // For each instance...
        val docid = lineSplit(0)
        val isSpam = if (lineSplit(1) == "spam") 1d else 0d   // label
        val features = lineSplit.drop(2).map(feature => feature.toInt) // feature vector of the training instance
        (0, (docid, isSpam, features))
    }).groupByKey(1)     // Then run the trainer...
    .flatMap(trainingItem => { // (Int, Iterable[(String, Double, Array[Int])])
        val iterables = trainingItem._2
        iterables.foreach(item => {  
            val isSpam = item._2
            val features = item._3

            val score = spamminess(features)
            val prob = 1.0 / (1 + exp(-score))
            features.foreach(f => {
              if (w.contains(f)) {
                w(f) += (isSpam - prob) * delta
              } else {
                w(f) = (isSpam - prob) * delta
              }
            })
          }
        )
        w
    })

    TRAINED.saveAsTextFile(args.model())

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    log.info(s"TrainSpamClassifier finished in: $duration seconds")

  }
}
