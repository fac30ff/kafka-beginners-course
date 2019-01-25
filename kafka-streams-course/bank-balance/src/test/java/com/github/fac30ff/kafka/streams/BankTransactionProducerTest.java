package com.github.fac30ff.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionProducerTest {
    @Test
    @SuppressWarnings(value = "")
    @Ignore
    public void newRandomTransactionsTest() {
        ProducerRecord<String, String> record = new BankTransactionProducer.newRandomTransaction("john");
        String key =record.key();
        String value = record.value();
        assertEquals(key, "jhon");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(value);
            assertEquals(jsonNode.get("name").asText(), "john");
            assertTrue("Amount should be less than 100: ", jsonNode.get("amount").asInt() < 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
