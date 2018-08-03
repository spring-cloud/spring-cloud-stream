/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.bootstrap;

import org.apache.kafka.streams.kstream.KStream;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * @author Soby Chacko
 */
@Ignore("Temporarily disabling the test as builds are getting slower due to this.")
public class KafkaStreamsBinderBootstrapTest {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10);

	@Test
	public void testKafkaStreamsBinderWithCustomEnvironmentCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.input.destination=foo",
						"--spring.cloud.stream.bindings.input.binder=kBind1",
						"--spring.cloud.stream.binders.kBind1.type=kstream",
						"--spring.cloud.stream.binders.kBind1.environment.spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
						"--spring.cloud.stream.binders.kBind1.environment.spring.cloud.stream.kafka.streams.binder.zkNodes=" + embeddedKafka.getZookeeperConnectionString());

		applicationContext.close();
	}

	@Test
	public void testKafkaStreamsBinderWithStandardConfigurationCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.input.destination=foo",
						"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
						"--spring.cloud.stream.kafka.streams.binder.zkNodes=" + embeddedKafka.getZookeeperConnectionString());

		applicationContext.close();
	}

	@SpringBootApplication
	@EnableBinding(StreamSourceProcessor.class)
	static class SimpleApplication {

		@StreamListener
		public void handle(@Input("input") KStream<Object, String> stream) {

		}
	}

	interface StreamSourceProcessor {
		@Input("input")
		KStream<?, ?> inputStream();
	}
}
