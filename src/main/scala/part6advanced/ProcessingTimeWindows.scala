package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, length, sum, window}
import org.apache.spark.sql.streaming.OutputMode

object ProcessingTimeWindows {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Processing Time Windows")
	  .master("local[2]")
	  .getOrCreate()

	def aggByProcessingTime(): Unit = {
		val linesCharCountByWindowDF = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()
		  .select(col("value"), current_timestamp().as("processingTime"))
		  .groupBy(window(col("processingTime"), "10 seconds").as("window"))
		  .agg(sum(length(col("value"))).as("charCount"))
		  .select(col("window").getField("start").as("start"),
			  col("window").getField("end").as("end"),
			  col("charCount"))

		linesCharCountByWindowDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		aggByProcessingTime()
	}
}
