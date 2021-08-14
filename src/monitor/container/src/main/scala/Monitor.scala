import org.apache.kafka.clients.admin.{
    Admin, 
    AdminClient, 
    AdminClientConfig, 
    LogDirDescription
}

import org.json4s.native.Json
import org.json4s.DefaultFormats
import java.time.LocalDateTime
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.{
    Map => mMap
}

object Monitor {
    def main(args: Array[String]) = {
        val adminClient = adminClientCreate()
        val topicsOfInterest = Set("delivery_events_v6_topic", "delivery_events_v7_topic")
        val partitionLeaders = getPartitionLeaders(adminClient, topicsOfInterest)
        val partitionBytes = getPartitionSize(adminClient, partitionLeaders)
        println(partitionBytes)
        val jsonString = Json(DefaultFormats).write(partitionBytes)
        println(jsonString)
        val epoch = System.currentTimeMillis
        println(LocalDateTime.now())

    }

    def adminClientCreate(): Admin = {
        val props = new Properties()
        props.putAll(
            Map(
                "bootstrap.servers" -> "18.202.250.11:9092" //prod:18.202.250.11 uat:52.213.38.208
            ).asJava
        )
        Admin.create(props)
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
