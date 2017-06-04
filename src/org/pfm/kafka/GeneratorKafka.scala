package org.pfm.kafka

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.io.Source
import java.io.{ FileReader, FileNotFoundException, IOException }
import java.util.concurrent.Future
import java.sql.Timestamp

object GeneratorKafka {

  def main(args: Array[String]) {

    // Static Values
    val myHostKafka = "localhost:9092"
    val myTopic = "kschool-topic"
    val myClientID = "KafkaProducer"
    val myNumberMessagesSec = 5
    val myfileName = "/tmp/dataset/libro_full.csv"

    //    if (args.length!=5){
    //      throw new IllegalArgumentException(s"Error number args. host topic clientID messagesPerSecond filename")
    //    }   

    val hostKafka = util.Try(args(0)).getOrElse(myHostKafka)
    val topic = util.Try(args(1)).getOrElse(myTopic)
    val clientID = util.Try(args(2)).getOrElse(myClientID)
    val numberMessagesSec: Double = util.Try(args(3).toDouble).getOrElse(myNumberMessagesSec)
    val myValue: Double = 1000
    val pause = myValue / numberMessagesSec
    val fileName: String = util.Try(args(4)).getOrElse(myfileName)
    
    println(s"Connecting to $topic")

    val props = new java.util.Properties()
    props.put("bootstrap.servers", hostKafka)
    props.put("client.id", clientID)
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val producer = new KafkaProducer[String, String](props)
    var bucleGo:Boolean = true
    while (bucleGo) {
      try {

        for (line <- Source.fromFile(fileName).getLines()) {
          val timestamp = new Timestamp(System.currentTimeMillis())
          val record = new ProducerRecord[String, String](topic, timestamp.getTime().toString(), line)
          val metaF: Future[RecordMetadata] = producer.send(record)

          val meta = metaF.get() // blocking!
          val msgLog =
            s"""
       |offset    = ${meta.offset()}
       |partition = ${meta.partition()}
       |topic     = ${meta.topic()}
     """.stripMargin
          println(msgLog)
          Thread.sleep(pause.toLong);
        }
      } catch {
        case ex: FileNotFoundException => println("File not found:" +myfileName )
                  bucleGo = false
        case ex: IOException => println("IOException reading file")
                  bucleGo = false
      }
    }

    producer.flush()
    producer.close()

  }


}