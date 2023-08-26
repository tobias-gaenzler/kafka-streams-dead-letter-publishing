package de.tg.kafka.streams.dead.letter.errorhandling;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

import static de.tg.kafka.streams.dead.letter.errorhandling.ErrorMessageMapper.ERROR_MESSAGE_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
import static org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL;
import static org.slf4j.LoggerFactory.getLogger;

public class DeadLetterDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private final Logger logger = getLogger(DeadLetterDeserializationExceptionHandler.class);
    private KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @Override
    public DeserializationHandlerResponse handle(
            ProcessorContext context,
            ConsumerRecord<byte[], byte[]> failingRecord,
            Exception exception) {
        String errorTopic = "deserialization-exception.DLT";
        byte[] key = failingRecord.key();
        byte[] value = failingRecord.value();
        try {
            logger.info("Exception while deserializing producerRecord with key {} in topic {}", key, failingRecord.topic());
            ProducerRecord<byte[], byte[]> resultRecord = new ProducerRecord<>(errorTopic, key, value);
            resultRecord.headers().add(ERROR_MESSAGE_HEADER, exception.getMessage().getBytes(UTF_8));

            kafkaTemplate.send(resultRecord);

            logger.info("Record was sent to dead letter topic {}, skipping message with key {}.", errorTopic, key);
            return CONTINUE;
        } catch (Exception e) {
            logger.error("Error while sending message with key {} to dead letter topic {}: {}", key, errorTopic, e.getMessage());
            return FAIL;
        }
    }

    @Override
    public void configure(Map<String, ?> map) {
        // this class is constructed by the kafka streams framework and not by spring,
        // hence we need to create a producer manually
        Map<String, Object> props = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, map.get(BOOTSTRAP_SERVERS_CONFIG),
                KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        ProducerFactory<byte[], byte[]> factory = new DefaultKafkaProducerFactory<>(props);
        this.kafkaTemplate = new KafkaTemplate<>(factory);
    }
}
