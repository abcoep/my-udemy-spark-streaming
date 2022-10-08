package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.Car
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}

object IntegratingCassandra {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Integrating Cassandra")
	  .master("local[2]")
	  .getOrCreate()
	import spark.implicits._

	def writeStreamToCassandraInBatches(): Unit = {
		val carsDS = spark.readStream
		  .schema(common.carsSchema)
		  .json("src/main/resources/data/cars")
		  .as[Car]

		carsDS.writeStream
		  .foreachBatch { (batch: Dataset[Car], _: Long) =>
			  batch.select(col("Name"), col("Horsepower"))
				.write
				.cassandraFormat("cars", "public")
				.mode(SaveMode.Append)
				.save()
		  }.start()
		  .awaitTermination()
	}

	class CarCassandraForeachWriter extends ForeachWriter[Car] {
		/*
			- on every batch, on every partition 'partitionId'
				- on every 'epoch' = chunk of data
					- call the open method; if false skip this chunk else open a Cassandra connection
					- for each entry in this epoch, call the process method
					- call the close method with success status from process at the end of the processing the epoch or pass error if obtained during processing
		 */
		val keyspace = "public"
		val table = "cars"
		val connector: CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

		override def open(partitionId: Long, epochId: Long): Boolean = {
			println(s"Opening ${this.getClass} connection")
			true
		}

		override def process(car: Car): Unit = {
			connector.withSessionDo { session =>
				session.execute(
					s"""
					   |insert into $keyspace.$table("Name", "Horsepower")
					   |values ('${car.Name}', ${car.Horsepower.orNull})
					   |""".stripMargin
				)
			}
		}

		override def close(errorOrNull: Throwable): Unit = println(s"Closing ${this.getClass} connection")
	}

	def writeStreamToCassandra(): Unit = {
		val carsDS = spark.readStream
		  .schema(common.carsSchema)
		  .json("src/main/resources/data/cars")
		  .as[Car]

		carsDS.writeStream
		  .foreach(new CarCassandraForeachWriter)
		  .start()
		  .awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		writeStreamToCassandra()
	}
}
