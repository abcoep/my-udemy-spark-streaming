package part3lowlevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStreams {
	// Discretized streams
	val spark = SparkSession.builder()
	  .appName("DStreams")
	  .master("local[2]")
	  .getOrCreate()

	/*
		Spark Streaming Context = entry point to the DStreams API
		- needs the spark context
		- a duration = batch itnerval
	 */
	val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

	/*
		- define input sources by creating DStreams
		- define transformations on DStreams
		- start all computations with ssc.start()
			- no more computations can be added after start
		- await termination, or stop the computation
			 you cannot restart the ssc
	 */

	def readFromSocket() = {
		val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

		val wordsStream: DStream[String] = socketStream.flatMap(_.split(" "))

		wordsStream.print()

		ssc.start()
		ssc.awaitTermination()
	}

	def createNewFile(fileName: String, dirPath: String) = {
		new Thread(() => {
			Thread.sleep(5000)
			val dir = new File(dirPath)
			val numFiles = dir.listFiles().length
			val newFile = new File(s"$dirPath${File.separator}${numFiles}_$fileName")
			newFile.createNewFile()

			val writer = new FileWriter(newFile)
			writer.write(
				"""
				  |GOOG,Aug 1 2004,102.37
				  |GOOG,Sep 1 2004,129.6
				  |GOOG,Oct 1 2004,190.64
				  |GOOG,Nov 1 2004,181.98
				  |GOOG,Dec 1 2004,192.79
				  |GOOG,Jan 1 2005,195.62
				  |""".stripMargin.trim)
			writer.close()
		}).start()
	}

	def readFromFiles() = {
		val dirPath = "src/main/resources/data/stocks"

		/*
			ssc.textFileStream monitors only a directory for new files, not existing files. Therefore createNewFile
		 */
		val stocksTextStream: DStream[String] = ssc.textFileStream(dirPath)

		val stocksStream = stocksTextStream.map { line =>
			val tokens = line.split(",")
			val company = tokens(0)
			val date = new Date(new SimpleDateFormat("MMM d yyyy").parse(tokens(1)).getTime)
			val price = tokens(2).toDouble

			Stock(company, date, price)
		}

		stocksStream.print()

		createNewFile("newStocks.csv", dirPath)

		ssc.start()
		ssc.awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		readFromFiles()
	}
}
