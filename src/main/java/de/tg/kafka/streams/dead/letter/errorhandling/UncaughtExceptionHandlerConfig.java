package de.tg.kafka.streams.dead.letter.errorhandling;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.lang.NonNull;

import static java.lang.Integer.MAX_VALUE;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;

@Configuration
public class UncaughtExceptionHandlerConfig {
    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsCustomizer() {
        return new StreamsBuilderFactoryBeanConfigurer() {
            @Override
            public void configure(@NonNull StreamsBuilderFactoryBean factoryBean) {
                factoryBean.setStreamsUncaughtExceptionHandler((throwable -> SHUTDOWN_APPLICATION));
            }

            @Override
            public int getOrder() {
                return MAX_VALUE;
            }
        };
    }
}
