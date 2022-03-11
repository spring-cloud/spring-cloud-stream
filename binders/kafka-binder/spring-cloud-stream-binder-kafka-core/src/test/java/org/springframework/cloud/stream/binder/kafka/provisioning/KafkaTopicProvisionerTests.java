/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.provisioning;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.SslChannelBuilder;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class KafkaTopicProvisionerTests {

	AdminClientConfigCustomizer adminClientConfigCustomizer = adminClientProperties -> adminClientProperties.put("foo", "bar");

	@SuppressWarnings("rawtypes")
	@Test
	public void bootPropertiesOverriddenExceptServers() throws Exception {
		KafkaProperties bootConfig = new KafkaProperties();
		bootConfig.getProperties().put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"PLAINTEXT");
		bootConfig.setBootstrapServers(Collections.singletonList("localhost:1234"));
		KafkaBinderConfigurationProperties binderConfig = new KafkaBinderConfigurationProperties(
				bootConfig);
		binderConfig.getConfiguration().put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
				"SSL");
		ClassPathResource ts = new ClassPathResource("test.truststore.ks");
		binderConfig.getConfiguration().put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
				ts.getFile().getAbsolutePath());
		binderConfig.setBrokers("localhost:9092");
		KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(binderConfig,
				bootConfig, adminClientConfigCustomizer);
		AdminClient adminClient = provisioner.createAdminClient();
		assertThat(KafkaTestUtils.getPropertyValue(adminClient,
				"client.selector.channelBuilder")).isInstanceOf(SslChannelBuilder.class);
		Map configs = KafkaTestUtils.getPropertyValue(adminClient,
				"client.selector.channelBuilder.configs", Map.class);
		assertThat(
				((List) configs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).get(0))
						.isEqualTo("localhost:1234");
		assertThat(configs.get("foo")).isEqualTo("bar");
		adminClient.close();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void bootPropertiesOverriddenIncludingServers() throws Exception {
		KafkaProperties bootConfig = new KafkaProperties();
		bootConfig.getProperties().put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"PLAINTEXT");
		bootConfig.setBootstrapServers(Collections.singletonList("localhost:9092"));
		KafkaBinderConfigurationProperties binderConfig = new KafkaBinderConfigurationProperties(
				bootConfig);
		binderConfig.getConfiguration().put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
				"SSL");
		ClassPathResource ts = new ClassPathResource("test.truststore.ks");
		binderConfig.getConfiguration().put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
				ts.getFile().getAbsolutePath());
		binderConfig.setBrokers("localhost:1234");
		KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(binderConfig,
				bootConfig, adminClientConfigCustomizer);
		AdminClient adminClient = provisioner.createAdminClient();
		assertThat(KafkaTestUtils.getPropertyValue(adminClient,
				"client.selector.channelBuilder")).isInstanceOf(SslChannelBuilder.class);
		Map configs = KafkaTestUtils.getPropertyValue(adminClient,
				"client.selector.channelBuilder.configs", Map.class);
		assertThat(
				((List) configs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).get(0))
						.isEqualTo("localhost:1234");
		adminClient.close();
	}

	@Test
	public void brokersInvalid() throws Exception {
		KafkaProperties bootConfig = new KafkaProperties();
		KafkaBinderConfigurationProperties binderConfig = new KafkaBinderConfigurationProperties(
				bootConfig);
		binderConfig.getConfiguration().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:1234");
		try {
			new KafkaTopicProvisioner(binderConfig, bootConfig, adminClientConfigCustomizer);
			fail("Expected illegal state");
		}
		catch (IllegalStateException e) {
			assertThat(e.getMessage()).isEqualTo(
					"Set binder bootstrap servers via the 'brokers' property, not 'configuration'");
		}
	}

}
