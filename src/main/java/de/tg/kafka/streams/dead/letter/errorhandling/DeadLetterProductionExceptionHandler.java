package de.tg.kafka.streams.dead.letter.errorhandling;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

import static de.tg.kafka.streams.dead.letter.errorhandling.ErrorMessageMapper.ERROR_MESSAGE_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE;
import static org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL;

public class DeadLetterProductionExceptionHandler implements ProductionExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(DeadLetterProductionExceptionHandler.class);
    private KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> failingRecord,
            Exception exception) {
        String errorTopic = "production-exception.DLT";
        byte[] key = failingRecord.key();
        byte[] value = getSaveValue(failingRecord, exception);
        try {
            logger.info("Exception while producing record with key {} to topic {}", key, failingRecord.topic());
            ProducerRecord<byte[], byte[]> resultRecord = new ProducerRecord<>(errorTopic, key, value);
            resultRecord.headers().add(ERROR_MESSAGE_HEADER, exception.getMessage().getBytes(UTF_8));

            kafkaTemplate.send(resultRecord);

            logger.info("Record was sent to dead letter topic {}, skipping message with key {}.", errorTopic, key);
            return CONTINUE;
        } catch (Exception e) {
            logger.error("Could not send message with key {} to dead letter topic {}: {}.", key, errorTopic, e.getMessage());
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

    private byte[] getSaveValue(ProducerRecord<byte[], byte[]> failingRecord, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            logger.info("Truncating value (too large).");
            // return empty value, as value is too large to send
            return new byte[0];
        } else {
            return failingRecord.value();
        }
    }
}
