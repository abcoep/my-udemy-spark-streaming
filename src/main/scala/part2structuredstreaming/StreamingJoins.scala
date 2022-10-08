package part2structuredstreaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SparkSession, functions}

object StreamingJoins {
	val spark = SparkSession.builder()
	  .appName("Streaming Joins")
	  .master("local[2]")
	  .getOrCreate()

	val guitarPlayers = spark.read
	  .option("inferSchema", true)
	  .json("src/main/resources/data/guitarPlayers")

	val guitars = spark.read
	  .option("inferSchema", true)
	  .json("src/main/resources/data/guitars")

	val bands = spark.read
	  .option("inferSchema", true)
	  .json("src/main/resources/data/bands")

	val joinCondition = guitarPlayers.col("band") === bands.col("id")
	val guitaristsBands = guitarPlayers.join(bands, joinCondition)

	def joinStreamWithStatic() = {
		val streamBandsDF = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load() // DF with single col "value"
		  .select(functions.from_json(col("value"), bands.schema).as("band"))
		  .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

		// join happens per batch
		val streamedBandsGuitaristsDF = streamBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamBandsDF.col("id"))

		/*
			restricted joins due to Spark not being able to track stream data from history to join again
			- stream-static join: right outer, full outer, right semi (right anti doesn't exist)
			- static-stream: left outer, full outer, left semi
		 */
		streamedBandsGuitaristsDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()
		  .awaitTermination()
	}

	def joinStreamWithStream() = {
		val streamBandsDF = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load() // DF with single col "value"
		  .select(functions.from_json(col("value"), bands.schema).as("band"))
		  .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

		val streamGuitaristsDF = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12346)
		  .load() // DF with single col "value"
		  .select(functions.from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
		  .selectExpr("guitarPlayer.id as id", "guitarPlayer.guitars as guitars", "guitarPlayer.name", "guitarPlayer.band as band")

		// join happens per batch
		val streamedBandsGuitaristsDF = streamBandsDF.join(streamGuitaristsDF, streamGuitaristsDF.col("band") === streamBandsDF.col("id"))

		/*
			- inner joins are supported
			- left/right outer joins are supported with watermark
			- full outer joins not supported
		 */

		streamedBandsGuitaristsDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append()) // only append mode supported for stream-stream join
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		joinStreamWithStream()
	}
}
