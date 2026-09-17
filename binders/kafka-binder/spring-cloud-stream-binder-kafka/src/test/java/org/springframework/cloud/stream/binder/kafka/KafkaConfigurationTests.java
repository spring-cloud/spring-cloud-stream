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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.micrometer.observation.ObservationRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.endpoint.BindingsEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author Fernando Blanch
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.cloud.function.definition=barConsumer;fooConsumer",
	"spring.kafka.listener.immediate-stop=true",
	"spring.cloud.stream.kafka.binder.enableObservation=true",
	"management.endpoint.bindings.enabled=true",
	"management.endpoints.web.exposure.include=bindings",
	"spring.cloud.stream.bindings.fooConsumer-in-0.destination=foo",
	"spring.cloud.stream.bindings.barConsumer-in-0.destination=bar",
	"spring.cloud.stream.bindings.barConsumer-in-0.group=bar-group"
})
@EmbeddedKafka
@DirtiesContext
public class KafkaConfigurationTests {

	private static final AtomicInteger RECEIVED_MESSAGES = new AtomicInteger();

	@Autowired
	private BindingService bindingService;

	@Autowired
	private KafkaTemplate<?, byte[]> kafkaTemplate;

	@Autowired
	private BindingsEndpoint bindingsEndpoint;

	@Test
	void testKafkaContainerConfigurationPropagation() throws Exception {
		Binding<?> fooDestination = this.bindingService.getConsumerBindings("fooConsumer-in-0").iterator().next();
		Map<String, Object> fooAdditionalConfigurationProperties = fooDestination.getAdditionalConfigurationProperties();
		assertThat(((Map) fooAdditionalConfigurationProperties.get("containerProperties")).get("stopImmediate")).isEqualTo(true);

		Binding<?> barDestination = this.bindingService.getConsumerBindings("barConsumer-in-0").iterator().next();
		Map<String, Object> barAdditionalConfigurationProperties = barDestination.getAdditionalConfigurationProperties();
		assertThat(((Map) barAdditionalConfigurationProperties.get("containerProperties")).get("stopImmediate")).isEqualTo(true);
	}

	@Test
	void testObservationRegistryIsRestoredAfterGettingAdditionalConfigurationProperties() {
		Binding<?> binding = this.bindingService.getConsumerBindings("barConsumer-in-0").iterator().next();
		DirectFieldAccessor bindingAccessor = new DirectFieldAccessor(binding);
		ObservationRegistry observationRegistry = (ObservationRegistry) bindingAccessor
				.getPropertyValue("lifecycle.messageListenerContainer.containerProperties.observationRegistry");

		this.bindingsEndpoint.queryStates();

		assertThat(bindingAccessor.getPropertyValue(
				"lifecycle.messageListenerContainer.containerProperties.observationRegistry"))
					.isSameAs(observationRegistry);
	}

	@Test
	void testKafkaConsumerCanBeRestartedAfterGettingAdditionalConfigurationProperties() {
		RECEIVED_MESSAGES.set(0);
		Binding<?> binding = this.bindingService.getConsumerBindings("barConsumer-in-0").iterator().next();

		this.bindingsEndpoint.queryStates();
		binding.stop();
		binding.start();

		this.kafkaTemplate.send("bar", null, "foo".getBytes());
		this.kafkaTemplate.flush();

		Awaitility.await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(RECEIVED_MESSAGES).hasValue(1));
	}

	@EnableAutoConfiguration
	@Configuration
	public static class Config {

		@Bean
		Consumer<String> barConsumer() {
			return message -> RECEIVED_MESSAGES.incrementAndGet();
		}
		@Bean
		Consumer<String> fooConsumer() {
			return message -> {
			};
		}
	}
}
