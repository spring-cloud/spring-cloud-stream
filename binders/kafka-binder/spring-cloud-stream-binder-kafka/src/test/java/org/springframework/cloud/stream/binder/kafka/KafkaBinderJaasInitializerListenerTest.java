/*
 * Copyright 2016-2023 the original author or authors.
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
import javax.security.auth.login.Configuration;

import com.sun.security.auth.login.ConfigFile;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
@EmbeddedKafka
class KafkaBinderJaasInitializerListenerTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static String JAVA_LOGIN_CONFIG_PARAM_VALUE;

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(KafkaBinderConfiguration.class, KafkaAutoConfiguration.class);

	@BeforeAll
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				embeddedKafka.getBrokersAsString());
		//Retrieve the current value for this system property if there is one set.
		JAVA_LOGIN_CONFIG_PARAM_VALUE = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
		System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
	}

	@AfterAll
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
		//If there was a previous value for this property, then restore it.
		if (JAVA_LOGIN_CONFIG_PARAM_VALUE != null) {
			System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, JAVA_LOGIN_CONFIG_PARAM_VALUE);
		}
	}

	@BeforeEach
	public void before() {
		System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
		// Invalidate any existing security configuration so that tests are forced to create/use fresh configuration.
		Configuration.setConfiguration(null);
	}

	@Test
	void configurationParsedCorrectlyWithKafkaClientAndDefaultControlFlag()
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
	void configurationParsedCorrectlyWithKafkaClientAndNonDefaultControlFlag()
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
