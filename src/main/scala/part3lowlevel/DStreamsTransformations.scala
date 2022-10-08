package part3lowlevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformations {
	val spark: SparkSession = SparkSession.builder()
	  .appName("DStreams Transformations")
	  .master("local[2]")
	  .getOrCreate()

	val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

	def readPeople(): DStream[Person] = ssc.socketTextStream("localhost", 9999).map { line =>
		val tokens = line.split(":")
		Person(
			tokens(0).toInt,
			tokens(1),
			tokens(2),
			tokens(3),
			tokens(4),
			Date.valueOf(tokens(5)),
			tokens(6),
			tokens(7).toInt
		)
	}

	// transformations - map, flatMap, filter
	def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
		val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
		(s"${person.firstName} ${person.lastName}", age)
	}

	def countNames(): DStream[(String, Long)] =
		readPeople().map(_.firstName)  .countByValue()

	def countNamesByReduce(): DStream[(String, Int)] =
		readPeople().map(person => (person.firstName, 1))
		  .reduceByKey(_ + _)

	def main(args: Array[String]): Unit = {
		val stream = countNamesByReduce()
		stream.print()
		ssc.start()
		ssc.awaitTermination()
	}
}
