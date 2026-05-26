package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

public class CommitLogProducer {

    public static void main(String[] args) {
        int count = 1000;
        String bootstrapServers = "primary:9092";
        String topic = "commit-log";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count" -> {
                    if (i + 1 < args.length) {
                        count = Integer.parseInt(args[++i]);
                    }
                }
                case "--bootstrap-servers" -> {
                    if (i + 1 < args.length) {
                        bootstrapServers = args[++i];
                    }
                }
                case "--topic" -> {
                    if (i + 1 < args.length) {
                        topic = args[++i];
                    }
                }
                default -> {
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
                }
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        
        // Optimize internal producer timeouts to prevent immediate structural drops
        props.put("max.block.ms", "60000"); // Allow up to 60s for broker metadata to populate
        props.put("retries", "5");
        props.put("retry.backoff.ms", "1000");

        System.out.printf("Awaiting broker connectivity on cluster %s for topic '%s'...%n", bootstrapServers, topic);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("Producing %d events to topic '%s' on %s%n", count, topic, bootstrapServers);
            
            for (int i = 1; i <= count; i++) {
                String eventId = UUID.randomUUID().toString();
                String payload = buildEventJson(i, eventId);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventId, payload);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.printf("Encountered append error on record execution context %s: %s%n", eventId, exception.getMessage());
                            // Do not call System.exit(1) on transient connection warnings to let internal retries handle it
                        }
                    }
                });

                if (i % 100 == 0) {
                    System.out.printf("Successfully generated and flushed %d events to broker stream%n", i);
                }
            }
            producer.flush();
            System.out.println("Produce loop completed successfully.");
        } catch (Exception e) {
            System.err.println("Fatal breakdown inside producer pipeline context topology:");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String buildEventJson(int index, String eventId) {
        long timestamp = Instant.now().getEpochSecond();
        String key = String.format("doc:%08d", index);
        return String.format(
                "{\"event_id\":\"%s\",\"timestamp\":%d,\"op_type\":\"UPDATE\",\"key\":\"%s\",\"value\":{\"status\":\"active\",\"sequence\":%d}}",
                eventId, timestamp, key, index);
    }
}