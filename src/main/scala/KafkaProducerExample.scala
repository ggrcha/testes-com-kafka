import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.Properties

object KafkaProducerExample {

  def main(args:Array[String]) {

    val props = new Properties()
    props.put("bootstrap.servers","127.0.0.1:9092")
    props.put("serializer.class","kafka.serializer.StringEncoder")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("linger.ms","1")

    val source = "refatoração do projeto"
    val producer = new KafkaProducer[String, String](props)
    val data = new ProducerRecord[String, String]("primeiro_topico", source)
    producer.send(data)
    producer.close()

  }

}
