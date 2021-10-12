import org.apache.kafka.clients.admin.{
    Admin, 
    AdminClient, 
    AdminClientConfig, 
    LogDirDescription
}
import org.apache.kafka.clients.producer.{
    KafkaProducer, 
    Producer,
    ProducerRecord,
    RecordMetadata
}


import org.json4s.native.Json
import org.json4s.DefaultFormats
import java.time.LocalDateTime
import java.util.Properties
import scala.util.control.Breaks._

import scala.collection.JavaConverters._
import scala.collection.mutable.{
    Map => mMap, 
    Queue
}

class Measurement(var partitionBytes: mMap[String, mMap[Int, Long]], var timestamp: Long) {
    def difference(first: Measurement): mMap[String, mMap[Int, Double]] = {
        val timediff = (this.timestamp - first.timestamp).toDouble/1000.toDouble
        val partitionBytesDiff = mMap[String, mMap[Int, Double]]()
        this.partitionBytes.foreach {
            case(topic, partitions) => {
                if(partitionBytesDiff.get(topic) == None)
                    partitionBytesDiff += (topic -> mMap[Int, Double]())

                partitions.foreach{
                    case(partition, secondPartitionSize) => {
                        val firstPartitionSize = first.partitionBytes.get(topic).get.get(partition).get
                        partitionBytesDiff(topic) += (partition -> (secondPartitionSize - firstPartitionSize)/timediff)
                    }
                }
            }
        }
        partitionBytesDiff
    }
}

object Monitor {

    def main(args: Array[String]) = {
        val adminClient = adminClientCreate("broker:29092") //prod:18.202.250.11 uat:52.213.38.208
        val producerClient = producerClientCreate("broker:29092")
        val topicsOfInterest = Set("monitor_speed_test")
        val tseries = Queue[Measurement]()

        while(true) {
            val currentTime = System.currentTimeMillis
            val partitionLeaders = getPartitionLeaders(adminClient, topicsOfInterest)
            val partitionBytes = getPartitionSize(adminClient, partitionLeaders)
            tseries += new Measurement(partitionBytes, currentTime)

            while( (currentTime-tseries.front.timestamp) > 30000) 
                tseries.dequeue

            if(tseries.size > 1) {
                val earliest = tseries.front
                val latest = tseries.last 
                if(latest.timestamp - earliest.timestamp > 25000) { 
                    println(latest.partitionBytes)
                    val writeSpeeds = latest.difference(earliest)
                    val jsonString = Json(DefaultFormats).write(writeSpeeds)
                    println(jsonString)

                    //val record = new ProducerRecord[String, String]("data-engineering-monitor", jsonString)
                    //producerClient.send(record).get()

                    producerClient.send(new ProducerRecord[String, String](
                        "monitor-speed", 
                        writeSpeeds.get("monitor_speed_test").get(0).toString
                    ))
                }
            }

            //val jsonString = Json(DefaultFormats).write(partitionBytes)
            //println(jsonString)

            Thread.sleep(50)
        }
    }

    def adminClientCreate(brokers: String): Admin = {
        val props = new Properties()
        props.putAll(
            Map(
                "bootstrap.servers" -> brokers 
            ).asJava
        )
        Admin.create(props)
    }
    
    def producerClientCreate(brokers: String): KafkaProducer[String, String] = {
        val props = new Properties()
        props.putAll(
            Map(
                "bootstrap.servers" -> brokers,
                "client.id" -> "data-engineering-monitor-producer",
                "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
            ).asJava
        )
        val producer = new KafkaProducer[String, String](props)
        producer
    }

    def getPartitionLeaders(
        adminClient: Admin, 
        topicsOfInterest: Set[String]
    ): mMap[String, mMap[Int, Int]] = {
        val partitionLeaders = mMap[String, mMap[Int, Int]]()
        val describeTopicsResult = adminClient.describeTopics(topicsOfInterest.toSeq.asJava).all.get().asScala.map{ 
            case(topicName, topicDescription) =>
                topicDescription.partitions.asScala.zipWithIndex.foreach{
                    case(partitionInfo, partition) => {
                        if (partitionLeaders.get(topicName) != None) {
                            var topicPartitions = partitionLeaders.get(topicName).get
                            topicPartitions += (partition -> partitionInfo.leader.id)
                        } else {
                            partitionLeaders += topicName -> (
                                mMap(partition -> partitionInfo.leader.id)
                            ) 
                        }
                    }
                }
        }
        partitionLeaders
    }

    def getPartitionSize(
        adminClient: Admin, 
        partitionLeaders: mMap[String, mMap[Int, Int]]
    ): mMap[String, mMap[Int, Long]] = {
        val clusterBrokers = adminClient.describeCluster().nodes().get().asScala.map(_.id()).toSet
        val partitionBytes = mMap[String, mMap[Int, Long]]()
        val describeLogDirsResult = adminClient.describeLogDirs(clusterBrokers.map(Integer.valueOf).toSeq.asJava)
        val logDirInfosByBroker = describeLogDirsResult.allDescriptions.get().asScala.map { 
            case (brokerId, submap) => 
                submap.asScala.map{
                    case(directoryPath, logDirDescription) => {
                        logDirDescription.replicaInfos.asScala.map{
                            case(topicPartition, replicaInfo) => {
                                if (partitionLeaders.get(topicPartition.topic) != None) {
                                    if (brokerId == partitionLeaders.get(topicPartition.topic).get.get(topicPartition.partition).get) {
                                        if (partitionBytes.get(topicPartition.topic) != None) {
                                            partitionBytes.get(topicPartition.topic).get += (topicPartition.partition -> replicaInfo.size)
                                        } else {
                                            partitionBytes += (topicPartition.topic -> mMap(topicPartition.partition -> replicaInfo.size))
                                        }
                                    }
                                }   
                            }
                        }
                    }
                }
        }
        partitionBytes
    }


}
