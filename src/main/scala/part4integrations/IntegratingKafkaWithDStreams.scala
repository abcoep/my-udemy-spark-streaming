package part4integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.jdk.CollectionConverters._

object IntegratingKafkaWithDStreams {
	val spark: SparkSession = SparkSession.builder()
	  .appName("Integrating Kafka with DStreams")
	  .master("local[2]")
	  .getOrCreate()

	val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

	val kafkaParams: Map[String, Object] = Map(
		"bootstrap.servers" -> "localhost:9092",
		"key.serializer" -> classOf[StringSerializer],
		"value.serializer" -> classOf[StringSerializer],
		"key.deserializer" -> classOf[StringDeserializer],
		"value.deserializer" -> classOf[StringDeserializer],
		"auto.offset.reset" -> "latest",
		"enable.auto.commit" -> java.lang.Boolean.FALSE
	)
	val kafkaParamsJavaMap = kafkaParams.asJava

	val kafkaTopic = "rockthejvm"

	def readFromKafka(): Unit = {
		val topics = Array(kafkaTopic)
		val kafkaDStream = KafkaUtils.createDirectStream(
			ssc,
			/*
				Evenly distributes partitions across all executors
				Alternatives:
				- PreferBrokers - if the brokers and executors are in the same cluster
				- PreferFixed
			 */
			LocationStrategies.PreferConsistent,
			/*
				Subscribe to a collection of topics
				Alternatives:
				- SubscribePattern - topics matching a pattern
				- Assign - advanced; allows specifying offsets and partitions per topic
			 */
			ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
		)

		val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
		processedStream.print()

		ssc.start()
		ssc.awaitTermination()
	}

	def writeToKafka(): Unit = {
		val inputData = ssc.socketTextStream("localhost", 12345)

		val processedData = inputData.map(_.toUpperCase)

		processedData.foreachRDD { rdd =>
			rdd.foreachPartition { partition =>
			  	// producer needs to be created locally at each executor handling a partition because it isn't serializable and therefore cannot be distributed to all executors by serializing and deserializing again. Also serde is more costly than just creating a new object
			  // Further reference: https://blog.allegro.tech/2015/08/spark-kafka-integration.html
				val producer = new KafkaProducer[String, String](kafkaParamsJavaMap)

				partition.foreach { record =>
					val message = new ProducerRecord[String, String](kafkaTopic, null, record)
					producer.send(message)
				}

				producer.close()
			}
		}

		ssc.start()
		ssc.awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		writeToKafka()
	}
}
