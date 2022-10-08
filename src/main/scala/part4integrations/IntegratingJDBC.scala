package part4integrations

import common.Car
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object IntegratingJDBC {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Integrating JDBC")
	  .master("local[2]")
	  .getOrCreate()
	import spark.implicits._

	val driver = "org.postgresql.Driver"
	val url = "jdbc:postgresql://localhost:5432/rtjvm"
	val user = "docker"
	val password = "docker"

	def writeStreamToPostgres(): Unit = {
		val carsDF = spark.readStream
		  .schema(common.carsSchema)
		  .json("src/main/resources/data/cars")

		val carsDS = carsDF.as[Car]

		carsDS.writeStream
		  .foreachBatch { (batch: Dataset[Car], _: Long) =>
			  batch.write
				.format("jdbc")
				.mode(SaveMode.Ignore)
				.options(Map(
					"driver" -> driver,
					"url" -> url,
					"user" -> user,
			  		"password" -> password,
					"dbtable" -> "public.cars"
				))
				.save()
		  }.start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		writeStreamToPostgres()
	}
}
