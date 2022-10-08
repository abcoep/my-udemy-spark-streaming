package part6advanced

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object EventTimeWindows {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Event Time Windows")
	  .master("local[2]")
	  .getOrCreate()

	val onlinePurchaseSchema = StructType(Array(
		StructField("id", StringType),
		StructField("time", TimestampType),
		StructField("item", StringType),
		StructField("quantity", IntegerType)
	))

	def readPurchasesFromSocket(): DataFrame = spark.readStream
	  .format("socket")
	  .option("host", "localhost")
	  .option("port", 12345)
	  .load()
	  .select(functions.from_json(col("value"), onlinePurchaseSchema).as("purchase"))
	  .selectExpr("purchase.*")

	def aggPurchasesByTumblingWindow(): Unit = {
		val purchasesDF = readPurchasesFromSocket()

		// tumbling window: windows don't overlap, they tumble. Therefore slide duration = window duration
		val windowByDay = purchasesDF
		  .groupBy(window(col("time"), "1 day").as("time")) // struct col with fields {start, end}
		  .agg(sum("quantity").as("totalQuantity"))
		  .select(
			  col("time").getField("start").as("start"),
			  col("time").getField("end").as("end"),
			  col("totalQuantity")
		  )

		windowByDay.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def aggPurchasesBySlidingWindow(): Unit = {
		val purchasesDF = readPurchasesFromSocket()

		val windowByDay = purchasesDF
		  .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct col with fields {start, end}
		  .agg(sum("quantity").as("totalQuantity"))
		  .select(
			  col("time").getField("start").as("start"),
			  col("time").getField("end").as("end"),
			  col("totalQuantity")
		  )

		windowByDay.writeStream
		  .format("console")
		  .outputMode(OutputMode.Complete())
		  .start()
		  .awaitTermination()
	}

	def readPurchasesFromFile(): DataFrame = spark.read
	  .schema(onlinePurchaseSchema)
	  .json("src/main/resources/data/purchases")

	def bestSellingProductPerDay(): Unit = {
		val purchasesDF = readPurchasesFromFile()

		val bestSellingProductPerDayDF = purchasesDF
		  .select(to_date(col("time")).as("date"), col("item"), col("quantity"))
		  .groupBy("date","item")
		  .agg(sum("quantity").as("totalQuantity"))
		  .withColumn("rank", rank().over(Window.partitionBy("date").orderBy(desc("totalQuantity"))))
		  .filter(col("rank") === 1)
		  .drop("rank")

		bestSellingProductPerDayDF.show()
	}

	def bestSellingProductPer24Hours(): Unit = {
		val purchasesDF = readPurchasesFromFile()

		val bestSellingProductPer24HoursDF = purchasesDF
		  .groupBy(window(col("time"), "1 day", "1 hour").as("24_hr_period"), col("item"))
		  .agg(sum("quantity").as("totalQuantity"))
		  .withColumn("rank", rank().over(Window.partitionBy("24_hr_period").orderBy(desc("totalQuantity"))))
		  .filter(col("rank") === 1)
		  .drop("rank")

		bestSellingProductPer24HoursDF.show()
	}

	def main(args: Array[String]): Unit = {
		bestSellingProductPerDay()
		bestSellingProductPer24Hours()
	}
}
