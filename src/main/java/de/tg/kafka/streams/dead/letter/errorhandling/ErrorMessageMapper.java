package de.tg.kafka.streams.dead.letter.errorhandling;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.io.Serializable;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ErrorMessageMapper<V extends Serializable, M> implements FixedKeyProcessor<String, MessageWrapper<V, M>, V> {
    public static final String ERROR_MESSAGE_HEADER = "error.message";
    private FixedKeyProcessorContext<String, V> context;

    @Override
    public void init(FixedKeyProcessorContext<String, V> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, MessageWrapper<V, M>> wrappedRecord) {
        String errorMessage = getErrorMessage(wrappedRecord);
        FixedKeyRecord<String, V> errorRecord = addErrorHeaderAndSetOriginalValue(wrappedRecord, errorMessage);
        context.forward(errorRecord);
    }

    private String getErrorMessage(FixedKeyRecord<String, MessageWrapper<V, M>> wrappedRecord) {
        Exception exception = wrappedRecord.value().exception();
        return exception.getClass().getName() + ": " + exception.getMessage();
    }

    private FixedKeyRecord<String, V> addErrorHeaderAndSetOriginalValue(
            FixedKeyRecord<String, MessageWrapper<V, M>> wrappedRecord,
            String errorMessage) {
        return wrappedRecord
                .withHeaders(wrappedRecord.headers().add(ERROR_MESSAGE_HEADER, errorMessage.getBytes(UTF_8)))
                .withValue(wrappedRecord.value().originalValue());
    }
}