package part2structuredstreaming

import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {

	val spark = SparkSession.builder()
	  .appName("Our first streams")
	  .master("local[2]")
	  .getOrCreate()

	def readFromSocket() = {
		// read stream DF
		val lines = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()

		// transformation
		val shortLines: DataFrame = lines.filter(length(col("value")) <= 20)

		println(shortLines.isStreaming)

		val query = shortLines.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()

		query.awaitTermination()
	}

	// will monitor folder for any new files and updates
	def readFromFiles() = {
		val stocksDF = spark.readStream
		  .format("csv")
		  .option("header", false)
		  .option("dateFormat", "MMM d yyyy")
		  .schema(common.stocksSchema)
		  .load("src/main/resources/data/stocks")

		stocksDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()
		  .awaitTermination()
	}

	def demoTriggers() = {
		val lines = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()

		// write the lines DF at a certain trigger
		lines.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .trigger(
			  Trigger.ProcessingTime(2.seconds) // every 2 secs run the query
//			  Trigger.Once() // single batch, then terminate
//			  Trigger.Continuous(2.seconds) // create a batch with a whatever is there every 2 secs
		  )
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		readFromSocket()
	}
}
