package de.tg.kafka.streams.dead.letter.errorhandling;

public record MessageWrapper<V, M>(V originalValue, M mappedValue, Exception exception) {

    public boolean hasError() {
        return exception != null;
    }

    public static final class Builder<V, M> {

        V originalValue;
        M mappedValue;
        Exception exception;

        public Builder<V, M> success(M mappedValue) {
            this.mappedValue = mappedValue;
            return this;
        }

        public Builder<V, M> fail(V originalValue, Exception exception) {
            this.originalValue = originalValue;
            this.exception = exception;
            return this;
        }

        public MessageWrapper<V, M> build() {
            return new MessageWrapper<>(originalValue, mappedValue, exception);
        }
    }
}
