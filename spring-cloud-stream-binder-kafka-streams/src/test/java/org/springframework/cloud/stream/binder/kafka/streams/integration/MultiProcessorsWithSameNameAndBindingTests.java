/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import org.apache.kafka.streams.kstream.KStream;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiProcessorsWithSameNameAndBindingTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	@Test
	public void testBinderStartsSuccessfullyWhenTwoProcessorsWithSameNamesAndBindingsPresent() {
		SpringApplication app = new SpringApplication(
				MultiProcessorsWithSameNameAndBindingTests.WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.bindings.input-1.destination=words",
				"--spring.cloud.stream.bindings.output.destination=counts",
				"--spring.cloud.stream.bindings.output.contentType=application/json",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {
			StreamsBuilderFactoryBean streamsBuilderFactoryBean1 = context
					.getBean("&stream-builder-Foo-process", StreamsBuilderFactoryBean.class);
			assertThat(streamsBuilderFactoryBean1).isNotNull();
			StreamsBuilderFactoryBean streamsBuilderFactoryBean2 = context
					.getBean("&stream-builder-Bar-process", StreamsBuilderFactoryBean.class);
			assertThat(streamsBuilderFactoryBean2).isNotNull();
		}
	}

	@EnableBinding(KafkaStreamsProcessorX.class)
	@EnableAutoConfiguration
	static class WordCountProcessorApplication {

		@Component
		static class Foo {
			@StreamListener
			public void process(@Input("input-1") KStream<Object, String> input) {
			}
		}

		//Second class with a stub processor that has the same name as above ("process")
		@Component
		static class Bar {
			@StreamListener
			public void process(@Input("input-1") KStream<Object, String> input) {
			}
		}
	}

	interface KafkaStreamsProcessorX {

		@Input("input-1")
		KStream<?, ?> input1();

	}
}
