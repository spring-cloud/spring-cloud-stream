/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.bootstrap;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 * @author Soby Chacko
 */
public class KafkaStreamsBinderBootstrapTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 10);

	@Test
	public void testKStreamBinderWithCustomEnvironmentCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.kafka.streams.bindings.input-1.consumer.application-id"
								+ "=testKStreamBinderWithCustomEnvironmentCanStart",
				"--spring.cloud.stream.kafka.streams.bindings.input-2.consumer.application-id"
						+ "=testKStreamBinderWithCustomEnvironmentCanStart-foo",
				"--spring.cloud.stream.kafka.streams.bindings.input-3.consumer.application-id"
						+ "=testKStreamBinderWithCustomEnvironmentCanStart-foobar",
						"--spring.cloud.stream.bindings.input-1.destination=foo",
						"--spring.cloud.stream.bindings.input-1.binder=kstream",
						"--spring.cloud.stream.binders.kstream.type=kstream",
						"--spring.cloud.stream.binders.kstream.environment"
								+ ".spring.cloud.stream.kafka.streams.binder.brokers"
								+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
				"--spring.cloud.stream.bindings.input-2.destination=bar",
				"--spring.cloud.stream.bindings.input-2.binder=ktable",
				"--spring.cloud.stream.binders.ktable.type=ktable",
				"--spring.cloud.stream.binders.ktable.environment"
						+ ".spring.cloud.stream.kafka.streams.binder.brokers"
						+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
				"--spring.cloud.stream.bindings.input-3.destination=foobar",
				"--spring.cloud.stream.bindings.input-3.binder=globalktable",
				"--spring.cloud.stream.binders.globalktable.type=globalktable",
				"--spring.cloud.stream.binders.globalktable.environment"
						+ ".spring.cloud.stream.kafka.streams.binder.brokers"
						+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		applicationContext.close();
	}

	@Test
	public void testKafkaStreamsBinderWithStandardConfigurationCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
				"--spring.cloud.stream.kafka.streams.bindings.input-1.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart",
				"--spring.cloud.stream.kafka.streams.bindings.input-2.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foo",
				"--spring.cloud.stream.kafka.streams.bindings.input-3.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foobar",
						"--spring.cloud.stream.kafka.streams.binder.brokers="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		applicationContext.close();
	}

	@SpringBootApplication
	@EnableBinding({SimpleKStreamBinding.class, SimpleKTableBinding.class, SimpleGlobalKTableBinding.class})
	static class SimpleKafkaStreamsApplication {

		@StreamListener
		public void handle(@Input("input-1") KStream<Object, String> stream) {

		}

		@StreamListener
		public void handleX(@Input("input-2") KTable<Object, String> stream) {

		}

		@StreamListener
		public void handleY(@Input("input-3") GlobalKTable<Object, String> stream) {

		}

	}

	interface SimpleKStreamBinding {

		@Input("input-1")
		KStream<?, ?> inputStream();

	}

	interface SimpleKTableBinding {

		@Input("input-2")
		KTable<?, ?> inputStream();

	}

	interface SimpleGlobalKTableBinding {

		@Input("input-3")
		GlobalKTable<?, ?> inputStream();

	}
}
