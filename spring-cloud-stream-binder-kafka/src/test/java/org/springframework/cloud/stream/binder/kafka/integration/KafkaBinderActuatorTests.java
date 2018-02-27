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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.search.Search;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 2.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = "spring.cloud.stream.bindings.input.group=" + KafkaBinderActuatorTests.TEST_CONSUMER_GROUP)
public class KafkaBinderActuatorTests {

	static final String TEST_CONSUMER_GROUP = "testGroup";

	private static final String KAFKA_BROKERS_PROPERTY = "spring.kafka.bootstrap-servers";

	@ClassRule
	public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY, kafkaEmbedded.getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private KafkaTemplate<?, byte[]> kafkaTemplate;

	@Test
	public void testKafkaBinderMetricsExposed() {
		Search search = this.meterRegistry.find(
				String.format("%s.%s.%s.lag", "spring.cloud.stream.binder.kafka", TEST_CONSUMER_GROUP, Sink.INPUT));

		assertThat(search.gauge()).isNotNull();

		this.kafkaTemplate.send(Sink.INPUT, null, "foo".getBytes());
		this.kafkaTemplate.flush();

		assertThat(search.gauge().value()).isGreaterThan(0);
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class KafkaMetricsTestConfig implements ApplicationListener<ApplicationReadyEvent> {

		@StreamListener(Sink.INPUT)
		public void process(String payload) throws InterruptedException {
			// Artificial slow listener to emulate consumer lag
			Thread.sleep(1000);
		}


		// TODO until the fix on the core level. See https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/250
		@Autowired
		private MeterRegistry meterRegistry;

		private final List<MeterBinder> springCloudStreamMeterBinders = new ArrayList<>();

		@Bean
		public DefaultBinderFactory.Listener listenerForMeterBinders() {

			return (configurationName, binderContext) -> {
				springCloudStreamMeterBinders.add(binderContext.getBean(MeterBinder.class));
			};

		}

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			this.springCloudStreamMeterBinders.forEach(b -> b.bindTo(this.meterRegistry));
		}

	}

}
