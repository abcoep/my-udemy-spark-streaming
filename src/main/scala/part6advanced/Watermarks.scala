package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import scala.concurrent.duration._
import java.sql.Timestamp

object Watermarks {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Late data with Watermarks")
	  .master("local[2]")
	  .getOrCreate()
	import spark.implicits._

	def debugQuery(query: StreamingQuery): Unit = {
		new Thread(() => {
			(1 to 100).foreach { i =>
				Thread.sleep(1000)
				val queryEventTime =
					if (query.lastProgress == null) "[]"
					else query.lastProgress.eventTime.toString

				println(s"$i: $queryEventTime")
			}
		}).start()
	}

	def testWatermark(): Unit = {
		val dataDF = spark.readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 12345)
		  .load()
		  .as[String]
		  .map { line =>
			  val tokens = line.split(",")
			  val timestamp = new Timestamp(tokens(0).toLong)
			  val data = tokens(1)

			  (timestamp, data)
		  }.toDF("created", "color")

		val watermarkedDF = dataDF
		  .withWatermark("created", "2 seconds")
		  .groupBy(window(col("created"), "2 seconds"), col("color"))
		  .count()
		  .selectExpr("window.*", "color", "count")

		/*
			A 2 seconds watermark means
			- a window will only be considered until the watermark surpasses the window end
			- an element/row/record will be considered if AFTER the watermark
		 */

		val query = watermarkedDF.writeStream
		  .format("console")
		  .outputMode(OutputMode.Append())
		  .trigger(Trigger.ProcessingTime(2.seconds))
		  .start()

		// not invoking awaitTermination provides an opportunity to debug and inspect the query state before writing to the output stream
		debugQuery(query)

		query.awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		testWatermark()
	}
}

object DataSender {
	val port = 12345
	val serverSocket = new ServerSocket(port)
	val socket = serverSocket.accept()
	val printer = new PrintStream(socket.getOutputStream)

	println(s"socket accepted at port $port")

	def sample1(): Unit = {
		Thread.sleep(7000)
		printer.println("7000,blue")
		Thread.sleep(1000)
		printer.println("8000,green")
		Thread.sleep(4000)
		printer.println("14000,blue")
		Thread.sleep(1000)
		printer.println("9000,red")
		Thread.sleep(3000)
		printer.println("15000,red")
		printer.println("8000,blue")
		Thread.sleep(1000)
		printer.println("13000,green")
		Thread.sleep(500)
		printer.println("21000,green")
		Thread.sleep(3000)
		printer.println("4000,purple")
		Thread.sleep(2000)
		printer.println("17000,green")
	}

	def sample2(): Unit = {
		printer.println("5000,red")
		printer.println("5000,green")
		printer.println("4000,blue")

		Thread.sleep(7000)
		printer.println("1000,yellow")
		printer.println("2000,cyan")
		printer.println("3000,magenta")
		printer.println("5000,black")

		Thread.sleep(3000)
		printer.println("10000,pink")
	}

	def sample3(): Unit = {
		Thread.sleep(2000)
		printer.println("9000,blue")
		Thread.sleep(3000)
		printer.println("2000,green")
		printer.println("1000,blue")
		printer.println("8000,red")
		Thread.sleep(2000)
		printer.println("5000,red")
		printer.println("18000,blue")
		Thread.sleep(1000)
		printer.println("2000,green")
		Thread.sleep(2000)
		printer.println("30000,purple")
		printer.println("10000,green")
	}

	def main(args: Array[String]): Unit = {
		sample3()
	}
}
