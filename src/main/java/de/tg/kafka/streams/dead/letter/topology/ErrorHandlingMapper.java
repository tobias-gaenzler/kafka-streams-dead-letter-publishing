package de.tg.kafka.streams.dead.letter.topology;

import de.tg.kafka.streams.dead.letter.errorhandling.MessageWrapper;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class ErrorHandlingMapper {
    private final Random random = new Random();

    public MessageWrapper<Integer, String> map(Integer stringLength) {
        try {
            String mappedValue = random
                    .ints(97, 123)
                    .limit(stringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            return new MessageWrapper.Builder<Integer, String>()
                    .success(mappedValue)
                    .build();
        } catch (Exception e) {
            return new MessageWrapper.Builder<Integer, String>()
                    .fail(stringLength, e)
                    .build();
        }
    }
}
