import org.apache.kafka.clients.consumer
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object KafkaConsumerExample {

  def main(args:Array[String]) {

    val props = new Properties()
    props.put("bootstrap.servers","127.0.0.1:9092")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id","teste")
    props.put("enable.auto.commit","true")
    props.put("auto.commit.interval.ms","1000")
    props.put("auto.offset.reset","earliest")

    val consumer = new KafkaConsumer[String,String](props)
    consumer.subscribe(Collections.singletonList("primeiro_topico"))

    while(true) {
      val reg = consumer.poll(100)
      for (x <- reg.asScala) {
        System.out.println("Received message: (" + x.key() + ", " + x.value() + ") at offset " + x.offset())
      }
    }
  }

}
