/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import javax.security.auth.login.AppConfigurationEntry;

import com.sun.security.auth.login.ConfigFile;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
public class KafkaBinderJaasInitializerListenerTest {

	@Test
	public void testConfigurationParsedCorrectlyWithKafkaClient() throws Exception {
		ConfigFile configFile = new ConfigFile(new ClassPathResource("jaas-sample-kafka-only.conf").getURI());
		final AppConfigurationEntry[] kafkaConfigurationArray = configFile.getAppConfigurationEntry(JaasUtils.LOGIN_CONTEXT_CLIENT);

		final ConfigurableApplicationContext context =
				SpringApplication.run(SimpleApplication.class,
						"--spring.cloud.stream.kafka.binder.jaas.options.useKeyTab=true",
						"--spring.cloud.stream.kafka.binder.jaas.options.storeKey=true",
						"--spring.cloud.stream.kafka.binder.jaas.options.keyTab=/etc/security/keytabs/kafka_client.keytab",
						"--spring.cloud.stream.kafka.binder.jaas.options.principal=kafka-client-1@EXAMPLE.COM");
		javax.security.auth.login.Configuration configuration = javax.security.auth.login.Configuration.getConfiguration();

		final AppConfigurationEntry[] kafkaConfiguration = configuration.getAppConfigurationEntry(JaasUtils.LOGIN_CONTEXT_CLIENT);
		assertThat(kafkaConfiguration).hasSize(1);
		assertThat(kafkaConfiguration[0].getOptions()).isEqualTo(kafkaConfigurationArray[0].getOptions());
		context.close();
	}

	@SpringBootApplication
	public static class SimpleApplication {

	}
}
