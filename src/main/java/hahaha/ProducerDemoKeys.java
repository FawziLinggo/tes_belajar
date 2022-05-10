package hahaha;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i<10;i++) {
            // create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("tesJava", "khun" + i);
            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record  is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was seccessfully data
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() +
                                "\n" + "Partisi :" + recordMetadata.partition() +
                                "\n" + "Offset :" + recordMetadata.offset() +
                                "\n" + "TimeStap :" + recordMetadata.timestamp());
                    } else {
                        logger.error("error while producing ", e);
                    }
                }
            });
        }
        //flush
        producer.flush();

        producer.close();
    }
}
