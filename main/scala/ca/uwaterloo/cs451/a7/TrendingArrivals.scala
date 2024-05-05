
package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream._

import scala.collection.mutable

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
    val output = args.output()

    def changeStateFunction: (Time, String, Option[Int], State[Map[String, Int]]) => Option[(String, (Int, Long, Int))] = (time, key, value, state) => {
      val stateMap = state.getOption.getOrElse(Map("goldman" -> 0, "citigroup" -> 0))

      val incoming = value.getOrElse(0)
      val prev = stateMap.getOrElse(key, 0)

      val updatedStateMap = stateMap.updated(key, incoming)
      state.update(updatedStateMap)

      Some((key, (incoming, time.milliseconds, prev)))
    }

    val stateSpec = StateSpec.function(changeStateFunction)

    val minLonCiti = -74.012083
    val maxLonCiti = -74.009867
    val minLatCiti = 40.720053
    val maxLatCiti = 40.7217236

    def withinCitiLimits(longitude: Double, latitude: Double): Boolean = {
      return (longitude <= maxLonCiti && minLonCiti <= longitude) && (latitude <= maxLatCiti && minLatCiti <= latitude)
    }


    val minLonGoldman = -74.0144185
    val maxLonGoldman = -74.013777
    val minLatGoldman = 40.7138745
    val maxLatGoldman = 40.7152275

    def withinGoldmanLimits(longitude: Double, latitude: Double): Boolean = {
      return (longitude <= maxLonGoldman) && (minLonGoldman <= longitude) && (latitude <= maxLatGoldman) && (minLatGoldman <= latitude)
    }

    val filteredStream = stream.map(_.split(","))
      .filter(tuple => {
        val isGreen = tuple(0) == "green"
        val idx1 = if(isGreen) 8 else 10
        val idx2 = if(isGreen) 9 else 11
        val longitude = tuple(idx1).toDouble
        val latitude = tuple(idx2).toDouble
        withinGoldmanLimits(longitude=longitude, latitude=latitude) || withinCitiLimits(longitude=longitude, latitude=latitude)
      })
      .map(tuple => {
        val isGreen = tuple(0) == "green"
        val idx1 = if(isGreen) 8 else 10
        val idx2 = if(isGreen) 9 else 11
        val longitude = tuple(idx1).toDouble
        val latitude = tuple(idx2).toDouble

        if(withinCitiLimits(longitude=longitude, latitude=latitude)){
            ("citigroup", 1)
        }
        else{
            ("goldman", 1)
        }
      }).reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, 
        (x: Int, y: Int) => x - y, 
        Minutes(10), 
        Minutes(10)
      )


    val stateStream = filteredStream.mapWithState(stateSpec).persist()

    stateStream.foreachRDD { (rdd, time) =>
      val formattedTime = "%08d".format(time.milliseconds)
      val outputPath = s"${output}/part-$formattedTime"

      val goldmanData = rdd.filter(_._1 == "goldman").collect()
      val citigroupData = rdd.filter(_._1 == "citigroup").collect()

      goldmanData.foreach {
        case ("goldman", (currentCount, time, prevCount)) =>
          if (currentCount >= 10 && currentCount >= 2 * prevCount) {
            println(s"Number of arrivals to Goldman Sachs has doubled from $prevCount to $currentCount at $time!")
          }
      }

      citigroupData.foreach {
        case ("citigroup", (currentCount, time, prevCount)) =>
          if (currentCount >= 10 && currentCount >= 2 * prevCount) {
            println(s"Number of arrivals to Citigroup has doubled from $prevCount to $currentCount at $time!")
          }
      }
      
      numCompletedRDDs.add(1L)
      rdd.saveAsTextFile(outputPath)
    }

    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
