package org.springframework.cloud.schema.avro;

import org.apache.avro.Schema;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.schema.avro.SubjectNamingStrategy;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by david.kalosi on 10/23/2017.
 */
public class SubjectNamingStrategyTest {

    static StubSchemaRegistryClient stubSchemaRegistryClient = new StubSchemaRegistryClient();

    @Test
    public void testCustomNamingStrategy() throws Exception{
        ConfigurableApplicationContext sourceContext = SpringApplication.run(AvroSourceApplication.class,
             "--server.port=0",
             "--debug",
             "--spring.jmx.enabled=false",
             "--spring.cloud.stream.bindings.output.contentType=application/*+avro",
             "--spring.cloud.stream.schema.avro.subjectNamingStrategy=org.springframework.cloud.schema.avro.CustomSubjectNamingStrategy",
             "--spring.cloud.stream.schema.avro.dynamicSchemaGenerationEnabled=true");

        Source source = sourceContext.getBean(Source.class);
        User1 user1 = new User1();
        user1.setFavoriteColor("foo" + UUID.randomUUID().toString());
        user1.setName("foo" + UUID.randomUUID().toString());
        source.output().send(MessageBuilder.withPayload(user1).build());

        MessageCollector barSourceMessageCollector = sourceContext.getBean(MessageCollector.class);
        Message<?> message = barSourceMessageCollector.forChannel(source.output()).poll(1000, TimeUnit.MILLISECONDS);

        assertThat(message.getHeaders().get("contentType"))
                .isEqualTo("application/vnd.org.springframework.cloud.schema.avro.User1.v1+avro");
    }

    @EnableBinding(Source.class)
    @EnableAutoConfiguration
    public static class AvroSourceApplication {

        @Bean
        public SchemaRegistryClient schemaRegistryClient() {
            return stubSchemaRegistryClient;
        }
    }
}
