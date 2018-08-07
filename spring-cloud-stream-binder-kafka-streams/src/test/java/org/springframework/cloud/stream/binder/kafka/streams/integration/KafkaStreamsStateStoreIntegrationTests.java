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

package org.springframework.cloud.stream.binder.kafka.streams.integration;


import java.util.Map;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Lei Chen
 * @author Soby Chacko
 */
public class KafkaStreamsStateStoreIntegrationTests {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "counts-id");

	@Test
	public void testKstreamStateStore() throws Exception {
		SpringApplication app = new SpringApplication(ProductCountApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=foobar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.bindings.input.consumer.headerMode=raw",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.kafka.streams.binder.zkNodes=" + embeddedKafka.getZookeeperConnectionString());
		try {
			Thread.sleep(2000);
			receiveAndValidateFoo(context);
		} catch (Exception e) {
			throw e;
		} finally {
			context.close();
		}
	}

	private void receiveAndValidateFoo(ConfigurableApplicationContext context) throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("foobar");
		template.sendDefault("{\"id\":\"123\"}");
		Thread.sleep(1000);

		//assertions
		ProductCountApplication productCount = context.getBean(ProductCountApplication.class);
		WindowStore<Object, String> state = productCount.state;
		assertThat(state != null).isTrue();
		assertThat(state.name()).isEqualTo("mystate");
		assertThat(state.persistent()).isTrue();
		assertThat(productCount.processed).isTrue();
	}

	@EnableBinding(KafkaStreamsProcessorX.class)
	@EnableAutoConfiguration
	public static class ProductCountApplication {

		WindowStore<Object, String> state;
		boolean processed;

		@StreamListener("input")
		@KafkaStreamsStateStore(name = "mystate", type = KafkaStreamsStateStoreProperties.StoreType.WINDOW, lengthMs = 300000)
		@SuppressWarnings({"deprecation", "unchecked"})
		public void process(KStream<Object, Product> input) {

			input
				.process(() -> new Processor<Object, Product>() {

					@Override
					public void init(ProcessorContext processorContext) {
						state = (WindowStore) processorContext.getStateStore("mystate");
					}

					@Override
					public void process(Object s, Product product) {
						processed = true;
					}

					@Override
					public void close() {
						if (state != null) {
							state.close();
						}
					}
				}, "mystate");
		}
	}

	public static class Product {

		Integer id;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}

	interface KafkaStreamsProcessorX {

		@Input("input")
		KStream<?, ?> input();
	}
}
