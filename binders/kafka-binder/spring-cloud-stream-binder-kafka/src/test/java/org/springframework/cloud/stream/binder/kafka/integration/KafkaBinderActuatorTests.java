/*
 * Copyright 2018-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.SanitizingFunction;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.ConsumerEndpointCustomizer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.cloud.stream.endpoint.BindingsEndpoint;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 * @author Oleg Zhurakousky
 * @author Jon Schneider
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 2.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
		"spring.cloud.stream.bindings.input.group=" + KafkaBinderActuatorTests.TEST_CONSUMER_GROUP,
		"spring.cloud.stream.function.bindings.process-in-0=input",
		"management.endpoints.web.exposure.include=bindings",
		"spring.cloud.stream.kafka.bindings.input.consumer.configuration.sasl.jaas.config=secret",
		"spring.cloud.stream.pollable-source=input"}
)
@DirtiesContext
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class KafkaBinderActuatorTests {

	static final String TEST_CONSUMER_GROUP = "testGroup-actuatorTests";

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private KafkaTemplate<?, byte[]> kafkaTemplate;

	@Autowired
	private ApplicationContext context;

	@Test
	void kafkaBinderMetricsExposed() {
		this.kafkaTemplate.send("input", null, "foo".getBytes());
		this.kafkaTemplate.flush();

		assertThat(this.meterRegistry.get("spring.cloud.stream.binder.kafka.offset")
				.tag("group", TEST_CONSUMER_GROUP).tag("topic", "input").gauge()
				.value()).isGreaterThan(0);
	}

	@Test
	@SuppressWarnings("unchecked")
	void bindingsActuatorEndpointInKafkaBinderBasedApp() {

		BindingsEndpoint controller = context.getBean(BindingsEndpoint.class);
		List<Map<String, Object>> bindings = controller.queryStates();
		Optional<Map<String, Object>> first = bindings.stream().filter(m -> m.get("bindingName").equals("input")).findFirst();
		assertThat(first.isPresent()).isTrue();
		Map<String, Object> inputBindingMap = first.get();

		Map<String, Object> extendedInfo = (Map<String, Object>) inputBindingMap.get("extendedInfo");
		Map<String, Object> extendedConsumerProperties = (Map<String, Object>) extendedInfo.get("ExtendedConsumerProperties");
		Map<String, Object> extension = (Map<String, Object>) extendedConsumerProperties.get("extension");
		Map<String, Object> configuration = (Map<String, Object>) extension.get("configuration");
		String saslJaasConfig = (String) configuration.get("sasl.jaas.config");

		assertThat(saslJaasConfig).isEqualTo("data-scrambled!!");

		List<Binding<?>> input = controller.queryState("input");
		// Since the above call goes through JSON serialization, we receive the type as a map of bindings.
		// The above call goes through this serialization because we provide a sanitization function.
		Map<String, Object> extendedInfo1 = (Map<String, Object>) ((Map<String, Object>) input.get(0)).get("extendedInfo");
		Map<String, Object> extendedConsumerProperties1 = (Map<String, Object>) extendedInfo1.get("ExtendedConsumerProperties");
		Map<String, Object> extension1 = (Map<String, Object>) extendedConsumerProperties1.get("extension");
		Map<String, Object> configuration1 = (Map<String, Object>) extension1.get("configuration");
		String saslJaasConfig1 = (String) configuration1.get("sasl.jaas.config");

		assertThat(saslJaasConfig1).isEqualTo("data-scrambled!!");
	}

	@Test
	@Disabled
	void kafkaBinderMetricsWhenNoMicrometer() {
		new ApplicationContextRunner().withUserConfiguration(KafkaMetricsTestConfig.class)
				.withPropertyValues(
						"spring.cloud.stream.bindings.input.group", KafkaBinderActuatorTests.TEST_CONSUMER_GROUP,
						"spring.cloud.stream.function.bindings.process-in-0", "input",
						"spring.cloud.stream.pollable-source", "input")
				.withClassLoader(new FilteredClassLoader("io.micrometer.core"))
				.run(context -> {
					assertThat(context.getBeanNamesForType(MeterRegistry.class))
							.isEmpty();
					assertThat(context.getBeanNamesForType(MeterBinder.class)).isEmpty();

					DirectFieldAccessor channelBindingServiceAccessor = new DirectFieldAccessor(
							context.getBean(BindingService.class));
					@SuppressWarnings("unchecked")
					Map<String, List<Binding<MessageChannel>>> consumerBindings =
						(Map<String, List<Binding<MessageChannel>>>) channelBindingServiceAccessor
							.getPropertyValue("consumerBindings");
					assertThat(new DirectFieldAccessor(
							consumerBindings.get("input").get(0)).getPropertyValue(
									"lifecycle.messageListenerContainer.beanName"))
											.isEqualTo("setByCustomizer:input");
					assertThat(new DirectFieldAccessor(
							consumerBindings.get("input").get(0)).getPropertyValue(
									"lifecycle.beanName"))
											.isEqualTo("setByCustomizer:input");
					assertThat(new DirectFieldAccessor(
							consumerBindings.get("source").get(0)).getPropertyValue(
									"lifecycle.beanName"))
											.isEqualTo("setByCustomizer:source");

					@SuppressWarnings("unchecked")
					Map<String, Binding<MessageChannel>> producerBindings =
						(Map<String, Binding<MessageChannel>>) channelBindingServiceAccessor
							.getPropertyValue("producerBindings");

					assertThat(new DirectFieldAccessor(
							producerBindings.get("output")).getPropertyValue(
							"lifecycle.beanName"))
							.isEqualTo("setByCustomizer:output");
				});
	}

	@EnableAutoConfiguration
	@Configuration
	public static class KafkaMetricsTestConfig {

		@Bean
		public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer() {
			return (c, q, g) -> c.setBeanName("setByCustomizer:" + q);
		}

		@Bean
		public MessageSourceCustomizer<KafkaMessageSource<?, ?>> sourceCustomizer() {
			return (s, q, g) -> s.setBeanName("setByCustomizer:" + q);
		}

		@Bean
		public ConsumerEndpointCustomizer<KafkaMessageDrivenChannelAdapter<?, ?>> consumerCustomizer() {
			return (p, q, g) -> p.setBeanName("setByCustomizer:" + q);
		}

		@Bean
		public ProducerMessageHandlerCustomizer<KafkaProducerMessageHandler<?, ?>> handlerCustomizer() {
			return (handler, destinationName) -> handler.setBeanName("setByCustomizer:" + destinationName);
		}

		@Bean
		public Consumer<String> process() {
			// Artificial slow listener to emulate consumer lag
			return s -> {
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					//no-op
				}
			};
		}

		@Bean
		public SanitizingFunction sanitizingFunction() {
			return sd -> {
				if (sd.getKey().equals("sasl.jaas.config")) {
					return sd.withValue("data-scrambled!!");
				}
				else {
					return sd;
				}
			};
		}

	}
}
