package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_two_verifier() throws Exception {
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");

        List<Double> firstFourAmounts = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        for (String line : transactionLines) {
            // Send the transaction line to Kafka
            kafkaProducer.send(line);

            // Parse CSV line: senderId, recipientId, amount
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                long senderId = Long.parseLong(parts[0].trim());
                long recipientId = Long.parseLong(parts[1].trim());
                float amount = Float.parseFloat(parts[2].trim());
                
                // Create Transaction object
                Transaction transaction = new Transaction(senderId, recipientId, amount);
                
                // Capture the amount
                firstFourAmounts.add((double) transaction.getAmount());

                // Stop after capturing first four amounts
                if (firstFourAmounts.size() >= 4) {
                    logger.info("First four transaction amounts: {}", firstFourAmounts.subList(0, 4));
                    System.out.println("First four transaction amounts: " + firstFourAmounts.subList(0, 4));
                    break;
                }
            }
        }
    }
}
