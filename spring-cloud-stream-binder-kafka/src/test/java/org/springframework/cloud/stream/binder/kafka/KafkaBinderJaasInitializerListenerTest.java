/*
 * Copyright 2016-2018 the original author or authors.
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

import javax.security.auth.login.AppConfigurationEntry;

import com.sun.security.auth.login.ConfigFile;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KafkaBinderJaasInitializerListenerTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true);


	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(KafkaBinderConfiguration.class, KafkaAutoConfiguration.class);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Before
	public void before() {
		System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
	}

	@Test
	public void testConfigurationParsedCorrectlyWithKafkaClientAndDefaultControlFlag()
			throws Exception {
		ConfigFile configFile = new ConfigFile(
				new ClassPathResource("jaas-sample-kafka-only.conf").getURI());
		final AppConfigurationEntry[] kafkaConfigurationArray = configFile
				.getAppConfigurationEntry("KafkaClient");


		this.contextRunner
				.withPropertyValues("spring.cloud.stream.kafka.binder.jaas.options.useKeyTab=true",
						"spring.cloud.stream.kafka.binder.jaas.options.storeKey=true",
						"spring.cloud.stream.kafka.binder.jaas.options.keyTab=/etc/security/keytabs/kafka_client.keytab",
						"spring.cloud.stream.kafka.binder.jaas.options.principal=kafka-client-1@EXAMPLE.COM",
						"spring.jmx.enabled=false")
				.run(context -> {
					javax.security.auth.login.Configuration configuration = javax.security.auth.login.Configuration
							.getConfiguration();

					final AppConfigurationEntry[] kafkaConfiguration = configuration
							.getAppConfigurationEntry("KafkaClient");
					assertThat(kafkaConfiguration).hasSize(1);
					assertThat(kafkaConfiguration[0].getOptions())
							.isEqualTo(kafkaConfigurationArray[0].getOptions());
					assertThat(kafkaConfiguration[0].getControlFlag())
							.isEqualTo(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED);
				});
	}

	@Test
	public void testConfigurationParsedCorrectlyWithKafkaClientAndNonDefaultControlFlag()
			throws Exception {
		ConfigFile configFile = new ConfigFile(
				new ClassPathResource("jaas-sample-kafka-only.conf").getURI());
		final AppConfigurationEntry[] kafkaConfigurationArray = configFile
				.getAppConfigurationEntry("KafkaClient");

		this.contextRunner
				.withPropertyValues("spring.cloud.stream.kafka.binder.jaas.options.useKeyTab=true",
						"spring.cloud.stream.kafka.binder.jaas.controlFlag=requisite",
						"spring.cloud.stream.kafka.binder.jaas.options.storeKey=true",
						"spring.cloud.stream.kafka.binder.jaas.options.keyTab=/etc/security/keytabs/kafka_client.keytab",
						"spring.cloud.stream.kafka.binder.jaas.options.principal=kafka-client-1@EXAMPLE.COM",
						"spring.jmx.enabled=false")
				.run(context -> {
					javax.security.auth.login.Configuration configuration = javax.security.auth.login.Configuration
							.getConfiguration();

					final AppConfigurationEntry[] kafkaConfiguration = configuration
							.getAppConfigurationEntry("KafkaClient");
					assertThat(kafkaConfiguration).hasSize(1);
					assertThat(kafkaConfiguration[0].getOptions())
							.isEqualTo(kafkaConfigurationArray[0].getOptions());
					assertThat(kafkaConfiguration[0].getControlFlag())
							.isEqualTo(AppConfigurationEntry.LoginModuleControlFlag.REQUISITE);
					context.close();
				});
	}
}
