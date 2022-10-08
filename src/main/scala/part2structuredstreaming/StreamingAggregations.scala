package part2structuredstreaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingAggregations {
	val spark = SparkSession.builder()
	  .appName("Streaming Aggregations")
	  .master("local[2]")
	  .getOrCreate()

	def streamingCount() = {
		val lines: DataFrame = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()

		val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

		// aggregations with distinct are not supported
		// otherwise Spark will need to keep track of everything
		lineCount.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete()) // append and update not supported for aggregations without using watermark
		  .start()
		  .awaitTermination()

	}

	def numericalAggregations(aggFunc: Column => Column): Unit = {
		val lines: DataFrame = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()

		val nums = lines.select(col("value").cast("integer").as("num"))
		val sumDF = nums.select(aggFunc(col("num")).as("sum_so_far"))

		sumDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def groupNames(): Unit = {
		val lines: DataFrame = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()

		val names = lines.select(col("value").as("name"))
		  .groupBy(col("name"))
		  .count()

		names.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		groupNames()
	}

}
