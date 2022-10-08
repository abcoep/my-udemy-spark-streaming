package part4integrations

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.streaming.OutputMode

object IntegratingKafka {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Integrating Kafka")
	  .master("local[2]")
	  .getOrCreate()
	import spark.implicits._

	/**
	 * docs: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
	 */
	def readFromKafka(): Unit = {
		val kafkaDF = spark.readStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "rockthejvm")
		  .load()
		  .select(col("topic"), expr("cast(value as string) as actualValue"))

		kafkaDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .start()
		  .awaitTermination()
	}

	def writeToKafka(): Unit = {
		val carsDF = spark.readStream
		  .schema(carsSchema)
		  .json("src/main/resources/data/cars")

		val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

		carsKafkaDF.writeStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("topic", "rockthejvm")
		  .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
		  .start()
		  .awaitTermination()
	}

	def writeCarsToKafka(): Unit = {
		val carsDF = spark.readStream
		  .schema(carsSchema)
		  .json("src/main/resources/data/cars")

		val carsKafkaDF = carsDF
		  .map(car => (car.getAs[String]("Name").toUpperCase, car.json))
		  .toDF("key", "value")

		carsKafkaDF.writeStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("topic", "rockthejvm")
		  .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		writeCarsToKafka()
	}
}
