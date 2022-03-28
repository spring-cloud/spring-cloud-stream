/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.kafka.streams.KeyValueSerdeResolver;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @author Soby Chacko
 * @author Eduard Domínguez
 */
public class KafkaStreamsBinderBootstrapTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 10);

	@Before
	public void before() {
		System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
	}

	@Test
	public void testKStreamBinderWithCustomEnvironmentCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.function.definition=input1;input2;input3",
						"--spring.cloud.stream.kafka.streams.bindings.input1-in-0.consumer.application-id"
								+ "=testKStreamBinderWithCustomEnvironmentCanStart",
				"--spring.cloud.stream.kafka.streams.bindings.input2-in-0.consumer.application-id"
						+ "=testKStreamBinderWithCustomEnvironmentCanStart-foo",
				"--spring.cloud.stream.kafka.streams.bindings.input3-in-0.consumer.application-id"
						+ "=testKStreamBinderWithCustomEnvironmentCanStart-foobar",
						"--spring.cloud.stream.bindings.input1-in-0.destination=foo",
						"--spring.cloud.stream.bindings.input1-in-0.binder=kstreamBinder",
						"--spring.cloud.stream.binders.kstreamBinder.type=kstream",
						"--spring.cloud.stream.binders.kstreamBinder.environment"
								+ ".spring.cloud.stream.kafka.streams.binder.brokers"
								+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
				"--spring.cloud.stream.bindings.input2-in-0.destination=bar",
				"--spring.cloud.stream.bindings.input2-in-0.binder=ktableBinder",
				"--spring.cloud.stream.binders.ktableBinder.type=ktable",
				"--spring.cloud.stream.binders.ktableBinder.environment"
						+ ".spring.cloud.stream.kafka.streams.binder.brokers"
						+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
				"--spring.cloud.stream.bindings.input3-in-0.destination=foobar",
				"--spring.cloud.stream.bindings.input3-in-0.binder=globalktableBinder",
				"--spring.cloud.stream.binders.globalktableBinder.type=globalktable",
				"--spring.cloud.stream.binders.globalktableBinder.environment"
						+ ".spring.cloud.stream.kafka.streams.binder.brokers"
						+ "=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		applicationContext.close();
	}

	@Test
	public void testKafkaStreamsBinderWithStandardConfigurationCanStart() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
				"--spring.cloud.function.definition=input1;input2;input3",
				"--spring.cloud.stream.kafka.streams.bindings.input1-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart",
				"--spring.cloud.stream.kafka.streams.bindings.input2-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foo",
				"--spring.cloud.stream.kafka.streams.bindings.input3-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foobar",
						"--spring.cloud.stream.kafka.streams.binder.brokers="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		applicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testStreamConfigGlobalProperties_GH1149() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
				"--spring.cloud.function.definition=input1;input2;input3",
				"--spring.cloud.stream.kafka.streams.bindings.input1-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart",
				"--spring.cloud.stream.kafka.streams.bindings.input2-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foo",
				"--spring.cloud.stream.kafka.streams.bindings.input2-in-0.consumer.configuration.spring.json.value.type.method=" + this.getClass().getName() + ".determineType",
				"--spring.cloud.stream.kafka.streams.bindings.input3-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foobar",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());

		Map<String, Object> streamConfigGlobalProperties = applicationContext
				.getBean("streamConfigGlobalProperties", Map.class);
		// Make sure that global stream configs do not contain individual binding config set on second function.
		assertThat(streamConfigGlobalProperties.containsKey("spring.json.value.type.method")).isFalse();

		// Make sure that only input2 function gets the specific binding property set on it.
		final StreamsBuilderFactoryBean input1SBFB = applicationContext.getBean("&stream-builder-input1", StreamsBuilderFactoryBean.class);
		final Properties streamsConfiguration1 = input1SBFB.getStreamsConfiguration();
		assertThat(streamsConfiguration1.containsKey("spring.json.value.type.method")).isFalse();

		final StreamsBuilderFactoryBean input2SBFB = applicationContext.getBean("&stream-builder-input2", StreamsBuilderFactoryBean.class);
		final Properties streamsConfiguration2 = input2SBFB.getStreamsConfiguration();
		assertThat(streamsConfiguration2.containsKey("spring.json.value.type.method")).isTrue();

		final StreamsBuilderFactoryBean input3SBFB = applicationContext.getBean("&stream-builder-input3", StreamsBuilderFactoryBean.class);
		final Properties streamsConfiguration3 = input3SBFB.getStreamsConfiguration();
		assertThat(streamsConfiguration3.containsKey("spring.json.value.type.method")).isFalse();
		applicationContext.getBean(KeyValueSerdeResolver.class);

		String configuredSerdeTypeResolver = (String) new DirectFieldAccessor(input2SBFB.getKafkaStreams())
				.getPropertyValue("taskTopology.processorNodes[0].valDeserializer.typeResolver.arg$2");

		assertThat(this.getClass().getName() + ".determineType").isEqualTo(configuredSerdeTypeResolver);

		String configuredKeyDeserializerFieldName = ((String) new DirectFieldAccessor(input2SBFB.getKafkaStreams())
				.getPropertyValue("taskTopology.processorNodes[0].keyDeserializer.typeMapper.classIdFieldName"));
		assertThat(DefaultJackson2JavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME).isEqualTo(configuredKeyDeserializerFieldName);

		String configuredValueDeserializerFieldName = ((String) new DirectFieldAccessor(input2SBFB.getKafkaStreams())
				.getPropertyValue("taskTopology.processorNodes[0].valDeserializer.typeMapper.classIdFieldName"));
		assertThat(DefaultJackson2JavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME).isEqualTo(configuredValueDeserializerFieldName);

		applicationContext.close();
	}

	public static JavaType determineType(byte[] data, Headers headers) {
		return TypeFactory.defaultInstance().constructParametricType(Map.class, String.class, String.class);
	}

	@SpringBootApplication
	static class SimpleKafkaStreamsApplication {

		@Bean
		public Consumer<KStream<Object, String>> input1() {
			return s -> {
				// No-op consumer
			};
		}

		@Bean
		public Consumer<KTable<Map<String, String>, Map<String, String>>> input2() {
			return s -> {
				// No-op consumer
			};
		}

		@Bean
		public Consumer<GlobalKTable<Object, String>> input3() {
			return s -> {
				// No-op consumer
			};
		}
	}
}
