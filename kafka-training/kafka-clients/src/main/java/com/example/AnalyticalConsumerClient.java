package com.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class AnalyticalConsumerClient {
    public static void main(String[] args) {

        Properties config=new Properties();
        config.put("bootstrap.servers","localhost:9092");
        config.put("group.id","analytical-consumer-group");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        // commit offset manually
        config.put("enable.auto.commit","false");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(config);

        consumer.subscribe(List.of("my-topic1"));

        while (true){
            var records=consumer.poll(java.time.Duration.ofMillis(100)); // FetchRequest
            for (var record:records){
                System.out.printf("Received record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d)\n",
                        record.key(), record.value(), record.partition(), record.offset());
                // Process the record (e.g., send notification)
                // After processing, commit the offset
                // delay to simulate processing
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                consumer.commitSync(); // Commiting Offset after processing each record
            }
        }
            // __consumer_offsets topic
    }
}
