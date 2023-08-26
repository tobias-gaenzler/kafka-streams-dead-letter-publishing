package de.tg.kafka.streams.dead.letter;

import de.tg.kafka.streams.dead.letter.config.TopicConfig;
import de.tg.kafka.streams.dead.letter.errorhandling.ErrorMessageMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.kafka.test.utils.KafkaTestUtils.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"input"}, // not really needed, but if removed, the test fails
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class ErrorHandlingTopologyIntegrationTest {

    private static final int MAX_CONSUMER_WAIT_TIME = 3;
    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private TopicConfig topicConfig;

    @BeforeEach
    public void setup() {
        if (broker.getTopics().isEmpty()) {
            broker.addTopics(
                    topicConfig.input(),
                    topicConfig.output(),
                    topicConfig.processException(),
                    topicConfig.deserializationException(),
                    topicConfig.productionException());
        }
    }

    @Test
    void testNoException() throws ExecutionException, InterruptedException {
        Consumer<String, String> outputConsumer = createConsumer(topicConfig.output(), StringDeserializer.class);

        produceRecord(2);

        ConsumerRecord<String, String> result = getSingleRecord(
                outputConsumer,
                topicConfig.output(),
                ofSeconds(MAX_CONSUMER_WAIT_TIME));
        assertEquals(2, result.value().length());
    }

    @Test
    void testProcessException() throws ExecutionException, InterruptedException {
        Consumer<String, Integer> deadLetterConsumer = createConsumer(topicConfig.processException(), IntegerDeserializer.class);

        produceRecord(-1);

        ConsumerRecord<String, Integer> resultRecord = getSingleRecord(
                deadLetterConsumer,
                topicConfig.processException(),
                ofSeconds(MAX_CONSUMER_WAIT_TIME));
        assertEquals(-1, resultRecord.value()); // the original message, which caused the error
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("java.lang.IllegalArgumentException: -1", new String(headerValue, UTF_8)); // error message
    }

    @Test
    void testDeserializationException() throws ExecutionException, InterruptedException {
        Consumer<byte[], byte[]> deadLetterConsumer = getDeadLetterConsumer();
        deadLetterConsumer.subscribe(Collections.singletonList(topicConfig.deserializationException()));


        // send "String" instead of expected "Integer" -> deserialization exception
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(topicConfig.input(), 0, "key", "10")).get();
        }

        ConsumerRecord<byte[], byte[]> resultRecord = getSingleRecord(
                deadLetterConsumer,
                topicConfig.deserializationException(),
                ofSeconds(MAX_CONSUMER_WAIT_TIME));
        assertEquals("10", new String(resultRecord.value(), UTF_8)); // the original message, which caused the error (value is a string, but an integer was expected)
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("Size of data received by IntegerDeserializer is not 4", new String(headerValue, UTF_8)); // error message
    }

    @Test
    void testProductionException() throws ExecutionException, InterruptedException {
        Consumer<byte[], byte[]> deadLetterConsumer = getDeadLetterConsumer();
        deadLetterConsumer.subscribe(Collections.singletonList(topicConfig.productionException()));

        produceRecord(2_000_000);

        ConsumerRecord<byte[], byte[]> resultRecord = getSingleRecord(
                deadLetterConsumer,
                topicConfig.productionException(),
                ofSeconds(MAX_CONSUMER_WAIT_TIME));
        assertEquals("", new String(resultRecord.value())); // value is empty, because the original value exceeded kafka max message size
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("The message is 2000091 bytes when serialized which is larger than 1048576," +
                        " which is the value of the max.request.size configuration.",
                new String(headerValue, UTF_8)); // error message
    }

    @NotNull
    private <D> Consumer<String, D> createConsumer(String topic, Class<?> valueDeserializer) {
        Map<String, Object> consumerProperties = consumerProps(randomUUID().toString(), "true", broker);
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        Consumer<String, D> consumer = new DefaultKafkaConsumerFactory<String, D>(consumerProperties).createConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    @NotNull
    private Consumer<byte[], byte[]> getDeadLetterConsumer() {
        Map<String, Object> consumerProperties = consumerProps(randomUUID().toString(), "true", broker);
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<byte[], byte[]>(consumerProperties).createConsumer();
    }

    @NotNull
    private Map<String, Object> getProducerProperties(Class<?> valueSerializer) {
        Map<String, Object> producerProperties = producerProps(broker);
        producerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return producerProperties;
    }


    private void produceRecord(int x) throws InterruptedException, ExecutionException {
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(getProducerProperties(IntegerSerializer.class))) {
            producer.send(new ProducerRecord<>(topicConfig.input(), 0, "key", x)).get();
        }
    }
}
