package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerClient {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        config.put("acks", "all");

//        delivery.timeout.ms
        config.put("delivery.timeout.ms", "30000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        List<String> keys = List.of("upi","neft","rtgs","imps","wallet");
        for (int i = 0; i < 1000; i++) {
            String key = keys.get(i % keys.size());
            String value = "value-" + i;
            ProducerRecord<String,String> record=new org.apache.kafka.clients.producer.ProducerRecord<>("my-topic1", key, value);
            producer.send(record,(metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf("Sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d)\n",
                            key, value, metadata.partition(), metadata.offset());
                }
            });
            // To simulate some delay
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();

    }

}
