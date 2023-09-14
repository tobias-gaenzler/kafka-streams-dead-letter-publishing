# Kafka Streams Dead Letter Publishing
There are excellent articles from confluent about [error handling](https://developer.confluent.io/courses/kafka-streams/error-handling/) in Kafka Streams.   
This blog post will add to these and provide a detailed example of publishing messages to [dead letter topics](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html) with Spring Kafka Streams.   
Appropriate tests are provided.  
Please be aware that this is just one way of [handling exceptions](https://www.confluent.io/blog/error-handling-patterns-in-kafka/) and might not apply to your use case.


In general, messages, produced to dead letter topics should provide some metadata regarding the error, e.g. error message and message context in the error message header.
This information can simplify handling the error (e.g. re-send corrected message, adjust business process, fix code, ... ) significantly.
Additionally, dead letter topics should be monitored and appropriate actions taken, when errors occur.
Kafka Streams is able to handle high traffic and dead letter topics can fill up pretty fast if there is some not temporary error.
So it would be best, to handle messages in dead letter topics automatically, e.g. retry, special code path, ... .
However, if the expected volume of messages in dead letter topics is very small, manual processing of these messages is feasible.


## Table of Contents
1. [Kafka Streams Error Handling](#kafka-streams-error-handling)
2. [Random String of Given Length](#random-string-of-given-length)
2. [Dead Letter Publishing of Processing Errors](#dead-letter-publishing-of-processing-errors)
2. [Dead Letter Publishing of Deserialization Errors](#dead-letter-publishing-of-deserialization-errors)
2. [Dead Letter Publishing of Producer Errors](#dead-letter-publishing-of-producer-errors)
2. [Conclusion](#conclusion)

## Kafka Streams Error Handling
Kafka Streams has three broad categories of errors: 
- consumer errors -> handled by the [DeserializationExceptionHandler](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#default-deserialization-exception-handler)
- processing (user logic) errors -> must be handled by the user to support dead letter publishing (otherwise _StreamsUncaughtExceptionHandler_ will restart/stop the application)
- producer errors -> handled by the [ProductionExceptionHandler](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#default-production-exception-handler)

We will need to implement dead letter publishing for all three categories, as there is no out of the box support in Kafka Streams.  
When producing to the dead letter topic fails, the application is stopped by the [StreamsUncaughtExceptionHandler](https://developer.confluent.io/tutorials/error-handling/kstreams.html) to prevent duplicate records.

![Dead Letter Publishing](https://github.com/tobias-gaenzler/kafka-streams-dead-letter-publishing/blob/main/KafkaStreamsDeadLetterPublishing.png?raw=true)


Side notes: 
- [Spring Cloud Stream](https://docs.spring.io/spring-cloud-stream-binder-kafka/docs/current/reference/html/spring-cloud-stream-binder-kafka.html#_handling_deserialization_exceptions_in_the_binder) supports dead letter topics for deserialization errors via configuration
- Plain Spring Kafka supports dead letter topics for process errors via the _[DeadLetterPublishingRecoverer]( [https://docs.spring.io/spring-kafka/docs/current/reference/html/#dead-letters])_ and when [retry is exhausted](https://www.baeldung.com/spring-retry-kafka-consumer) 

## Random String of Given Length
We implement a Spring Kafka Streams application which receives a string length (_Integer_) from the _input_ topic and generates a random string with that length to the _output_ topic.  
The implementation without error handling:
```java
@Component
public class Topology {

    //...
    
    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {

        streamsBuilder
            .stream(topicConfig.input(), Consumed.with(String(), Integer()))
            .mapValues(stringLength -> new Random()
                .ints(97, 123)
                .limit(stringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString())
            .to(topicConfig.output(), Produced.with(String(), String()));
    }
}
```

## Dead Letter Publishing of Processing Errors
Can you guess what can go wrong?  
Yes, if we provide e.g. negative input, then an exception will be thrown, as we can not create strings with negative length.
This will then stop the application and streaming is blocked.

To solve this, we implement a _MessageWrapper_, a simple record containing the original value, the mapped value and the exception.
To support different value types, the _MessageWrapper_ is typed and we provide a _Builder_ for convenience.
```java
public record MessageWrapper<V, M>(V originalValue, M mappedValue, Exception exception) {

    public boolean hasError() {
        return exception != null;
    }
    
    public static final class Builder<V, M> {
        // ...

        public Builder<V, M> success(M mappedValue) {
            this.mappedValue = mappedValue;
            return this;
        }

        public Builder<V, M> fail(V originalValue, Exception exception) {
            this.originalValue = originalValue;
            this.exception = exception;
            return this;
        }

        //...
    }
}
```
After that, we catch all possible exceptions during the mapping and wrap the result of the mapping into the _MessageWrapper_:
```java
public class ErrorHandlingMapper {

    public MessageWrapper<Integer, String> map(Integer stringLength) {
        try {
            String mappedValue = new Random()
                    .ints(97, 123)
                    .limit(stringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            return new Builder<Integer, String>()
                    .success(mappedValue)
                    .build();
        } catch (Exception e) {
            return new Builder<Integer, String>()
                    .fail(stringLength, e)
                    .build();
        }
    }
}
```
and then split and branch the stream and send failed records to the dead letter topic and successful records to the output topic.
Before producing the message to the dead letter topic, we need to _unwrap_ the value and add error information.
For the branching syntax we use _branch( ..., withConsumer(...))_ and we can continue with the fluent statement (easier to read): 
```java
public class ErrorHandlingTopology {

    //...

    @Autowired
    public void buildErrorHandlingTopology(StreamsBuilder streamsBuilder) {

        streamsBuilder
            .stream(topicConfig.input(), Consumed.with(String(), Integer()))
            .mapValues(errorHandlingMapper::map) // the "wrapping" mapper
            .split() // split in error/no error stream
            .branch((key, value) -> value.hasError(), // failing records
                withConsumer(
                    stream ->
                        stream
                            // "unwrap" value and add error information to header
                            .processValues(() -> new ErrorMessageMapper<>())
                            // send to dead letter topic
                            .to(topicConfig.processException(), Produced.with(String(), Integer()))))
            .defaultBranch( // successful records
                withConsumer(
                    stream ->
                        stream
                            // extract the mapped value    
                            .mapValues(MessageWrapper::mappedValue)
                            // send to output topic    
                            .to(topicConfig.output(), Produced.with(String(), String()))));
    }
}
```

When you have multiple steps in your topology, you need to decide how to handle failure, e.g. _fail-fast_: you route to the dead letter topic as soon as you face an error or _fail-last_: wait till the end of the topology before sending the message to the dead letter topic.

Caveats:  
State is not supported, as a manual revert of the state would be required (which is not always possible).

We implement two kinds of tests for the process exception: 
- using [_TopologyTestDriver_](https://www.confluent.io/de-de/blog/test-kafka-streams-with-topologytestdriver/), which mocks Kafka (and is faster than using testcontainers):
```java
@SpringBootTest
class ErrorHandlingTopologyTest {
    
    //...
    
    @Autowired
    private ErrorHandlingTopology errorHandlingTopology;

    @Test
    void testProcessException() {
        produceRecord(new TestRecord<>("key", -1)); // use TopologyTestDriver to send a message

        assertTrue(outputTopic.isEmpty()); // No message in output topic. The failing message was sent to the dead letter topic.
        TestRecord<String, Integer> resultRecord = processExceptionTopic.readRecord();
        assertEquals(-1, resultRecord.value()); // the original message value, which caused the error
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("java.lang.IllegalArgumentException: -1", new String(headerValue, UTF_8)); // error message in header
    }
 
    private void produceRecord(TestRecord<String, Integer> input) {
        // build the topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        errorHandlingTopology.buildErrorHandlingTopology(streamsBuilder);
        topology = streamsBuilder.build();
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology);
             Serde<String> stringSerde = Serdes.String();
             Serde<Integer> integerSerde = Serdes.Integer()) {
            // create required topics
            inputTopic = testDriver.createInputTopic(config.input(), stringSerde.serializer(), integerSerde.serializer());
            outputTopic = testDriver.createOutputTopic(config.output(), stringSerde.deserializer(), stringSerde.deserializer());
            processExceptionTopic = testDriver.createOutputTopic(config.processException(), stringSerde.deserializer(), integerSerde.deserializer());

            inputTopic.pipeInput(input); // send message
        }
    }
}
```
- using [testcontainers](https://java.testcontainers.org/modules/kafka), which starts a real Kafka container and enables us to test other error cases (see _Deserialization Errors_ below):
```java
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"input"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class ErrorHandlingTopologyIntegrationTest {
    
    //...
    
    @Test
    void testProcessException() throws ExecutionException, InterruptedException {
        Consumer<String, Integer> deadLetterConsumer = createConsumer(topicConfig.processException(), IntegerDeserializer.class);

        produceRecord(-1); // use a real KafkaProducer to send the message

        ConsumerRecord<String, Integer> resultRecord = getSingleRecord(
                deadLetterConsumer,
                topicConfig.processException(),
                ofSeconds(MAX_CONSUMER_WAIT_TIME));
        assertEquals(-1, resultRecord.value()); // the original message value, which caused the error
        byte[] headerValue = resultRecord.headers().lastHeader(ErrorMessageMapper.ERROR_MESSAGE_HEADER).value();
        assertEquals("java.lang.IllegalArgumentException: -1", new String(headerValue, UTF_8)); // error message in header
    }

    private void produceRecord(int x) throws InterruptedException, ExecutionException {
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(getProducerProperties(IntegerSerializer.class))) {
            producer.send(new ProducerRecord<>(topicConfig.input(), 0, "key", x)).get();
        }
    }
}
```

## Dead Letter Publishing of Deserialization Errors
So now that errors during processing are sent to the dead letter topic, how do we handle deserialization errors?
Deserialization errors occur, when the _format/data type_ of the key or value is not correct.
Since our application expects an _Integer_ as input value, we can send a _String_ value instead, which will cause a deserialization exception.  
A common approach, to prevent such errors, is to use a [_schema registry_](https://docs.confluent.io/platform/current/schema-registry/index.html) and define schemas. 
This will ensure, that all data matches the schemas and reduce serialization/deserialization errors.


Handling deserialization errors is quite different from handling errors during processing.
We need to configure a handler in our properties (_application.yml_):
```yaml
spring:
  kafka:
    streams:
      properties:
        default:
          deserialization:
            exception:
              handler: "de.tg.kafka.streams.dead.letter.errorhandling.DeadLetterDeserializationExceptionHandler"
```

and provide the implementation.
The handler is instantiated by the Kafka Streams framework which makes it difficult to wire dependencies.
Hence, we have to create the producer to the dead letter topic ourselves.
```java
    @Override
    public void configure(Map<String, ?> map) {
        Map<String, Object> props = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, map.get(BOOTSTRAP_SERVERS_CONFIG),
                KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        ProducerFactory<byte[], byte[]> factory = new DefaultKafkaProducerFactory<>(props);
        this.kafkaTemplate = new KafkaTemplate<>(factory);
    }
```

Now the handler can send the failing record to the dead letter topic.
To prevent message loss, we return _DeserializationHandlerResponse.FAIL_, if producing to the dead letter topic fails and the application is stopped in that case.  
Please note, that the handler receives byte arrays for key and value, which is a good thing, as deserializing these values failed. We then send these raw byte arrays to the dead letter topic.   
Defining a schema on deserialization exception dead letter topics seems pointless, as we would run into the same deserialization error which caused the exception.
However, defining a schema on a dead letter topic for processing errors (see above) can be useful, e.g. to make the message data readable for manual error processing in a UI/3rd party application.

```java
public class DeadLetterDeserializationExceptionHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(
            ProcessorContext context,
            ConsumerRecord<byte[], byte[]> failingRecord,
            Exception exception) {
        try {
            ProducerRecord<byte[], byte[]> resultRecord = new ProducerRecord<>("deserialization-exception.DLT", failingRecord.key(), failingRecord.value());
            resultRecord.headers().add(ERROR_MESSAGE_HEADER, exception.getMessage().getBytes(UTF_8));
            kafkaTemplate.send(resultRecord);
            return CONTINUE;
        } catch (Exception e) {
            return FAIL;
        }
    }
```

We use test containers for testing the deserialization errors, because _TopologyTestDriver_ does not behave like a real Kafka regarding deserialization. 

## Dead Letter Publishing of Producer Errors
Let's discuss the last error category: _Producer Errors_.  
Producer errors are caused by different exceptions, e.g. retries exhausted or message is too large. The implementation should depend on the type of error, which caused the producer error.  
When your retries are exhausted (you reached the limit of [delivery.timeout.ms](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#retries)), it would make sense to re-produce after some grace period, as this is probably only a temporary error (no dead letter topic needed).   
When the message we want to produce is too large, sending it to the dead letter topic will also fail.  
However, in that case, it still might be beneficial, e.g. for monitoring, if we receive a message in the dead letter topic providing some details of the message, e.g. key, source topic, source partition and offset, ... .  
This data might then help to analyze the failure and correct the error.

Handling producer errors is very similar to handling deserialization errors.
We again configure the handler:
```yaml
spring:
  kafka:
    streams:
      properties:
        default:
          production:
            exception:
              handler: "de.tg.kafka.streams.dead.letter.errorhandling.DeadLetterProductionExceptionHandler"
```
Then we create the producer (see deserialization exception handler above) and then send the failing message to the dead letter topic. 
Here, we remove the value (see _getSaveValue()_), if it is too large (otherwise we would not be able to send to the dead letter topic):
```java
public class DeadLetterProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> failingRecord,
            Exception exception) {
        byte[] value = getSaveValue(failingRecord, exception);
        try {
            ProducerRecord<byte[], byte[]> resultRecord = new ProducerRecord<>("production-exception.DLT", failingRecord.key(), value);
            resultRecord.headers().add(ERROR_MESSAGE_HEADER, exception.getMessage().getBytes(UTF_8));
            kafkaTemplate.send(resultRecord);
            return CONTINUE;
        } catch (Exception e) {
            return FAIL;
        }
    }

    private byte[] getSaveValue(ProducerRecord<byte[], byte[]> failingRecord, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            // return empty value, as value is too large to send
            return new byte[0];
        } else {
            return failingRecord.value();
        }
    }
}
```

We use test containers for testing the producer errors, because _TopologyTestDriver_ does not behave like a real Kafka regarding producer errors.
The default size restriction in Kafka is one MB per message.
In the test we request a string of length _2,000,000_ which will trigger the producer exception.

# Conclusion
We have implemented one example of dead letter topic publishing with Spring Kafka Streams  
and provided integrative tests using test containers to show that all scenarios are handled as expected.  
With this implementation, failing records (caused by deserialization, processing or production errors) can be inspected on a separate topic,  
with original payload and diagnostic details available in the message headers.

### Restrictions:
Please note, that this example only works for stateless streams and simple topologies.  
No automated way of processing records in the dead letter topic is provided and processing order is not guaranteed  
(see e.g. retry topics if you need ordered processing).

# Thanks
The completion of this article would not have been possible without the support of:

- Confluent Inc.: Daniel Petisme
- TODO

Thank you all very much!