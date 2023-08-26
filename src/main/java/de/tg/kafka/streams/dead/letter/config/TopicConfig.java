package de.tg.kafka.streams.dead.letter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("topics")
public record TopicConfig(
        String input,
        String output,
        String processException,
        String deserializationException,
        String productionException
) {}