package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Emits N JSON events to commit-log and exits.
 * Schema matches the project spec: event_id, timestamp, op_type, key, value.
 */
public class CommitLogProducer {

    private static final String[] OP_TYPES = {"INSERT", "UPDATE", "DELETE"};

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int count = 1000;
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = "commit-log";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count": count = Integer.parseInt(args[++i]); break;
                case "--bootstrap-servers": bootstrap = args[++i]; break;
                case "--topic": topic = args[++i]; break;
                case "-h":
                case "--help":
                    System.out.println("Usage: --count N [--bootstrap-servers host:port] [--topic name]");
                    return;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(2);
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("linger.ms", "5");

        System.out.printf("Producing %d events to %s on %s%n", count, topic, bootstrap);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String eventId = UUID.randomUUID().toString();
                String key = "doc:" + Integer.toHexString(i);
                String op = OP_TYPES[i % OP_TYPES.length];
                String json = buildEvent(eventId, key, op);
                producer.send(new ProducerRecord<>(topic, eventId, json));
                if (i % 100 == 0) {
                    System.out.printf("  sent %d/%d%n", i, count);
                }
            }
            // flush happens on close, but be explicit
            producer.flush();
        }
        System.out.println("Done.");
    }

    private static String buildEvent(String eventId, String key, String op) {
        long ts = Instant.now().getEpochSecond();
        return "{\"event_id\":\"" + eventId + "\""
             + ",\"timestamp\":" + ts
             + ",\"op_type\":\"" + op + "\""
             + ",\"key\":\"" + key + "\""
             + ",\"value\":{\"status\":\"active\"}}";
    }
}
