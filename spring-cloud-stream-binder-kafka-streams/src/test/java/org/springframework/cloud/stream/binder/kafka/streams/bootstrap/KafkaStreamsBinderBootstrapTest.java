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

import java.util.function.Consumer;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
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
	public void testKafkaStreamsBinderJaasInitialization() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleKafkaStreamsApplication.class).web(WebApplicationType.NONE).run(
				"--spring.cloud.function.definition=input1;input2;input3",
				"--spring.cloud.stream.kafka.streams.bindings.input1-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-jaas",
				"--spring.cloud.stream.kafka.streams.bindings.input2-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foo-jaas",
				"--spring.cloud.stream.kafka.streams.bindings.input3-in-0.consumer.application-id"
						+ "=testKafkaStreamsBinderWithStandardConfigurationCanStart-foobar-jaas",
				"--spring.cloud.stream.kafka.streams.binder.jaas.loginModule=org.apache.kafka.common.security.plain.PlainLoginModule",
				"--spring.cloud.stream.kafka.streams.binder.jaas.options.username=foo",
				"--spring.cloud.stream.kafka.streams.binder.jaas.options.password=bar",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getEmbeddedKafka().getBrokersAsString());
		javax.security.auth.login.Configuration configuration = javax.security.auth.login.Configuration
				.getConfiguration();
		final AppConfigurationEntry[] kafkaConfiguration = configuration
				.getAppConfigurationEntry("KafkaClient");
		assertThat(kafkaConfiguration).hasSize(1);
		assertThat(kafkaConfiguration[0].getOptions().get("username")).isEqualTo("foo");
		assertThat(kafkaConfiguration[0].getOptions().get("password")).isEqualTo("bar");
		assertThat(kafkaConfiguration[0].getControlFlag())
				.isEqualTo(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED);
		applicationContext.close();
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
		public Consumer<KTable<Object, String>> input2() {
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
