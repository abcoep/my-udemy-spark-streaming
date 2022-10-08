package part2structuredstreaming

import common.Car
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.streaming.OutputMode

object StreamingDatasets {
	val spark = SparkSession.builder()
	  .appName("Streaming Datasets")
	  .master("local[2]")
	  .getOrCreate()
	import spark.implicits._

	def readCars(): Dataset[Car] = {
		val carEncoder: Encoder[Car] = Encoders.product[Car]

		spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()
		  .select(functions.from_json(col("value"), common.carsSchema).as("car")) // composite column - StructType
		  .selectExpr("car.*") // DF with multiple columns
		  .as[Car](carEncoder)
	}

	def showCarNames() = {
		val carsDS: Dataset[Car] = readCars()

		val carNamesDF: DataFrame = carsDS.select(col("Name"))

		val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

		carNamesAlt.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()
		  .awaitTermination()
	}

	def showPowerfulCars(): Unit = {
		// Powerful cars (> 140 HP)
		readCars().filter(_.Horsepower.getOrElse(0L) > 140)
		  .writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()
		  .awaitTermination()
	}

	def showAvgHP(): Unit = {
		// Aggregations - require outputMode "complete"
		readCars().agg(avg(col("Horsepower")))
		  .writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def showCarCountByOrigin(): Unit = {
		// Group by - also require outputMode "complete"
		readCars().groupByKey(_.Origin)
		  .count()
		  .writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
//		showCarNames()
//		showPowerfulCars()
//		showAvgHP()
		showCarCountByOrigin()
	}
}
