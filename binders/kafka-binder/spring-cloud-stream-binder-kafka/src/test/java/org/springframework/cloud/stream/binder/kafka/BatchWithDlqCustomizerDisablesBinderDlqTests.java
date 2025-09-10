/*
 * Copyright 2024-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.cloud.function.definition=consumer1",
	"spring.cloud.stream.bindings.consumer-in-0.group=batchWithDlqCustomizerDisablesBinderDlq",
	"spring.cloud.stream.kafka.bindings.consumer1-in-0.consumer.enable-dlq=true"})
@EmbeddedKafka
@DirtiesContext
public class BatchWithDlqCustomizerDisablesBinderDlqTests {

	@Autowired
	private DefaultBinderFactory binderFactory;

	@Test
	void batchWithDlqCustomizerDisablesBinderDlq() {
		KafkaMessageChannelBinder kafka = (KafkaMessageChannelBinder) this.binderFactory.getBinder("kafka", MessageChannel.class);

		KafkaConsumerProperties kafkaConsumerProperties =
			kafka.getExtendedConsumerProperties("consumer1-in-0");
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties =
			new ExtendedConsumerProperties<>(kafkaConsumerProperties);
		extendedConsumerProperties.setBatchMode(true);

		MessageHandler errorMessageHandler =
			kafka.getErrorMessageHandler(null, null, extendedConsumerProperties);
		// verifies that binder does not create a message handler for errors, which otherwise creates a handler for DLQ.
		assertThat(errorMessageHandler).isNull();
	}

	@EnableAutoConfiguration
	@Configuration
	public static class BatchWithDlqDisablesBinderDlqTestsConfig {

		@Bean
		Consumer<Message<List<String>>> consumer1() {
			return message -> {
			};
		}

		@Bean
		ListenerContainerWithDlqAndRetryCustomizer customizer() {
			return (container, destinationName, group, dlqDestinationResolver, backOff) -> {
			};
		}
	}

}
