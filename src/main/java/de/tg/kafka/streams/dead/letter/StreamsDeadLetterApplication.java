package de.tg.kafka.streams.dead.letter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@ConfigurationPropertiesScan
public class StreamsDeadLetterApplication {
	public static void main(String[] args) {
		SpringApplication.run(StreamsDeadLetterApplication.class, args);
	}
}
