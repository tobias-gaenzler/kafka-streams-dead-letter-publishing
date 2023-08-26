package de.tg.kafka.streams.dead.letter.topology;

import de.tg.kafka.streams.dead.letter.config.TopicConfig;
import de.tg.kafka.streams.dead.letter.errorhandling.ErrorMessageMapper;
import de.tg.kafka.streams.dead.letter.errorhandling.MessageWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.kstream.Branched.withConsumer;

@Component
public class ErrorHandlingTopology {

    private final TopicConfig topicConfig;
    private final ErrorHandlingMapper mapper;

    public ErrorHandlingTopology(TopicConfig topicConfig, ErrorHandlingMapper mapper) {
        this.topicConfig = topicConfig;
        this.mapper = mapper;
    }

    @Autowired
    public void buildErrorHandlingTopology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(topicConfig.input(), Consumed.with(String(), Integer()))
                .mapValues(mapper::map)
                .split()
                .branch((key, value) -> value.hasError(),
                        withConsumer(
                                stream ->
                                        stream
                                                .processValues(() -> new ErrorMessageMapper<>())
                                                .to(topicConfig.processException(), Produced.with(String(), Integer()))))
                .defaultBranch(
                        withConsumer(
                                stream ->
                                        stream
                                                .mapValues(MessageWrapper::mappedValue)
                                                .to(topicConfig.output(), Produced.with(String(), String()))));
    }
}
