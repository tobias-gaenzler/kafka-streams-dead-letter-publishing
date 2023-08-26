package de.tg.kafka.streams.dead.letter;

import de.tg.kafka.streams.dead.letter.config.TopicConfig;
import de.tg.kafka.streams.dead.letter.errorhandling.ErrorMessageMapper;
import de.tg.kafka.streams.dead.letter.topology.ErrorHandlingTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class ErrorHandlingTopologyTest {

    private TestInputTopic<String, Integer> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private TestOutputTopic<String, Integer> processExceptionTopic;
    private Topology topology;
    @Autowired
    private TopicConfig config;
    @Autowired
    private ErrorHandlingTopology errorHandlingTopology;

    @BeforeEach
    void clear() {
        inputTopic = null;
        processExceptionTopic = null;
        outputTopic = null;
        topology = null;
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        errorHandlingTopology.buildErrorHandlingTopology(streamsBuilder);
        topology = streamsBuilder.build();
    }

    @Test
    void testNoException() {
        produceRecord(new TestRecord<>("key", 2));

        TestRecord<String, String> resultRecord = outputTopic.readRecord();
        assertEquals(2, resultRecord.value().length());
    }

    @Test
    void testProcessException() {
        produceRecord(new TestRecord<>("key", -1));

        assertTrue(outputTopic.isEmpty()); // No message in output topic. The failing message was sent to the dead letter topic.
        TestRecord<String, Integer> resultRecord = processExceptionTopic.readRecord();
        assertEquals(-1, resultRecord.value()); // the original message, which caused the error
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("java.lang.IllegalArgumentException: -1", new String(headerValue, UTF_8)); // error message
    }

    private void produceRecord(TestRecord<String, Integer> input) {
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology);
             Serde<String> stringSerde = Serdes.String();
             Serde<Integer> integerSerde = Serdes.Integer()) {
            inputTopic = testDriver.createInputTopic(config.input(), stringSerde.serializer(), integerSerde.serializer());
            outputTopic = testDriver.createOutputTopic(config.output(), stringSerde.deserializer(), stringSerde.deserializer());
            processExceptionTopic = testDriver.createOutputTopic(config.processException(), stringSerde.deserializer(), integerSerde.deserializer());

            inputTopic.pipeInput(input);
        }
    }
}
