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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaBinderConfigurationPropertiesTest {

	@Test
	public void mergedConsumerConfigurationFiltersGroupIdFromKafkaProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.getConsumer().setGroupId("group1");
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);

		Map<String, Object> mergedConsumerConfiguration =
				kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	public void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.getConsumer().setEnableAutoCommit(true);
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);

		Map<String, Object> mergedConsumerConfiguration =
				kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
	}

	@Test
	public void mergedConsumerConfigurationFiltersGroupIdFromKafkaBinderConfigurationPropertiesConfiguration() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConfiguration(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "group1"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	public void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaBinderConfigurationPropertiesConfiguration() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConfiguration(Collections.singletonMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	public void mergedConsumerConfigurationFiltersGroupIdFromKafkaBinderConfigurationPropertiesConsumerProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConsumerProperties(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "group1"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	public void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaBinderConfigurationPropertiesConsumerProps() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConsumerProperties(Collections.singletonMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	public void testCertificateFilesAreConvertedToAbsolutePathsFromClassPathResources() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		final Map<String, String> configuration = kafkaBinderConfigurationProperties.getConfiguration();
		configuration.put("ssl.truststore.location", "classpath:testclient.truststore");
		configuration.put("ssl.keystore.location", "classpath:testclient.keystore");

		kafkaBinderConfigurationProperties.getKafkaConnectionString();

		assertThat(configuration.get("ssl.truststore.location"))
				.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.truststore").toString());
		assertThat(configuration.get("ssl.keystore.location"))
				.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.keystore").toString());
	}

	@Test
	public void testCertificateFilesAreConvertedToGivenAbsolutePathsFromClassPathResources() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		final Map<String, String> configuration = kafkaBinderConfigurationProperties.getConfiguration();
		configuration.put("ssl.truststore.location", "classpath:testclient.truststore");
		configuration.put("ssl.keystore.location", "classpath:testclient.keystore");
		kafkaBinderConfigurationProperties.setCertificateStoreDirectory("target");

		kafkaBinderConfigurationProperties.getKafkaConnectionString();

		assertThat(configuration.get("ssl.truststore.location")).isEqualTo(
				Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(configuration.get("ssl.keystore.location")).isEqualTo(
				Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());
	}

	@Test
	public void testCertificateFilesAreMovedForSchemaRegistryConfiguration() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		final Map<String, String> configuration = kafkaBinderConfigurationProperties.getConfiguration();
		configuration.put("schema.registry.ssl.truststore.location", "classpath:testclient.truststore");
		configuration.put("schema.registry.ssl.keystore.location", "classpath:testclient.keystore");
		kafkaBinderConfigurationProperties.setCertificateStoreDirectory("target");

		kafkaBinderConfigurationProperties.getKafkaConnectionString();

		assertThat(configuration.get("schema.registry.ssl.truststore.location")).isEqualTo(
				Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(configuration.get("schema.registry.ssl.keystore.location")).isEqualTo(
				Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());
	}
}
