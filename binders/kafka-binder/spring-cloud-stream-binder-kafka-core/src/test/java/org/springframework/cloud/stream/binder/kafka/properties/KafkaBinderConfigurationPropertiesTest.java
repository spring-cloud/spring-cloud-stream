/*
 * Copyright 2018-2024 the original author or authors.
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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaBinderConfigurationPropertiesTest {

	@Test
	void mergedConsumerConfigurationFiltersGroupIdFromKafkaProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.getConsumer().setGroupId("group1");
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);

		Map<String, Object> mergedConsumerConfiguration =
				kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.getConsumer().setEnableAutoCommit(true);
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);

		Map<String, Object> mergedConsumerConfiguration =
				kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
	}

	@Test
	void mergedConsumerConfigurationFiltersGroupIdFromKafkaBinderConfigurationPropertiesConfiguration() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConfiguration(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "group1"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaBinderConfigurationPropertiesConfiguration() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConfiguration(Collections.singletonMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	void mergedConsumerConfigurationFiltersGroupIdFromKafkaBinderConfigurationPropertiesConsumerProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConsumerProperties(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "group1"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	void mergedConsumerConfigurationFiltersEnableAutoCommitFromKafkaBinderConfigurationPropertiesConsumerProps() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
				new KafkaBinderConfigurationProperties(kafkaProperties);
		kafkaBinderConfigurationProperties
				.setConsumerProperties(Collections.singletonMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));

		Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();

		assertThat(mergedConsumerConfiguration).doesNotContainKeys(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Test
	void certificateFilesAreConvertedToAbsolutePathsFromClassPathResources() {
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
		deleteTempCertFiles();
	}

	@Test
	void certificateFilesAreConvertedToAbsolutePathsFromHttpResources() throws IOException {
		HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 5869), 0);
		createContextWithCertFileHandler(server, "testclient.truststore");
		createContextWithCertFileHandler(server, "testclient.keystore");
		server.setExecutor(null); // creates a default executor
		server.start();

		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
			new KafkaBinderConfigurationProperties(kafkaProperties);
		final Map<String, String> configuration = kafkaBinderConfigurationProperties.getConfiguration();
		configuration.put("ssl.truststore.location", "http://localhost:5869/testclient.truststore");
		configuration.put("ssl.keystore.location", "http://localhost:5869/testclient.keystore");
		configuration.put("schema.registry.ssl.truststore.location", "http://localhost:5869/testclient.truststore");
		configuration.put("schema.registry.ssl.keystore.location", "http://localhost:5869/testclient.keystore");

		kafkaBinderConfigurationProperties.getKafkaConnectionString();

		assertThat(configuration.get("ssl.truststore.location"))
			.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.truststore").toString());
		assertThat(configuration.get("ssl.keystore.location"))
			.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.keystore").toString());
		assertThat(configuration.get("schema.registry.ssl.truststore.location"))
			.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.truststore").toString());
		assertThat(configuration.get("schema.registry.ssl.keystore.location"))
			.isEqualTo(Paths.get(System.getProperty("java.io.tmpdir"), "testclient.keystore").toString());
		deleteTempCertFiles();

		server.stop(0);
	}

	@Test
	void certificateFilesAreConvertedToGivenAbsolutePathsFromClassPathResources() {
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
	void certificateFilesAreMovedForSchemaRegistryConfiguration() {
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

		final Map<String, Object> mergedProducerConfiguration = kafkaBinderConfigurationProperties.mergedProducerConfiguration();
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.truststore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.keystore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());

		final Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.truststore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.keystore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());
	}

	@Test
	void schemaRegistryPropertiesPropagatedToMergedProducerProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties =
			new KafkaBinderConfigurationProperties(kafkaProperties);
		final Map<String, String> configuration = kafkaBinderConfigurationProperties.getConfiguration();

		configuration.put("schema.registry.url", "https://localhost:8081,https://localhost:8082");
		configuration.put("schema.registry.ssl.truststore.location", "classpath:testclient.truststore");
		configuration.put("schema.registry.ssl.keystore.location", "classpath:testclient.keystore");
		configuration.put("schema.registry.ssl.keystore.password", "generated");
		configuration.put("schema.registry.ssl.truststore.password", "generated");
		configuration.put("schema.registry.ssl.key.password", "generated");

		kafkaBinderConfigurationProperties.setCertificateStoreDirectory("target");
		kafkaBinderConfigurationProperties.getKafkaConnectionString();

		final Map<String, Object> mergedProducerConfiguration = kafkaBinderConfigurationProperties.mergedProducerConfiguration();
		assertThat(mergedProducerConfiguration.get("schema.registry.url")).isEqualTo("https://localhost:8081,https://localhost:8082");
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.keystore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.truststore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.keystore.password")).isEqualTo("generated");
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.truststore.password")).isEqualTo("generated");
		assertThat(mergedProducerConfiguration.get("schema.registry.ssl.key.password")).isEqualTo("generated");


		final Map<String, Object> mergedConsumerConfiguration = kafkaBinderConfigurationProperties.mergedConsumerConfiguration();
		assertThat(mergedConsumerConfiguration.get("schema.registry.url")).isEqualTo("https://localhost:8081,https://localhost:8082");
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.keystore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.keystore").toString());
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.truststore.location")).isEqualTo(
			Paths.get(Files.currentFolder().toString(), "target", "testclient.truststore").toString());
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.keystore.password")).isEqualTo("generated");
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.truststore.password")).isEqualTo("generated");
		assertThat(mergedConsumerConfiguration.get("schema.registry.ssl.key.password")).isEqualTo("generated");


	}

	private void createContextWithCertFileHandler(HttpServer server, String path) {
		server.createContext("/" + path, exchange -> {
			ClassPathResource ts = new ClassPathResource(path);
			byte[] response = ts.getContentAsByteArray();
			exchange.sendResponseHeaders(200, response.length);
			OutputStream os = exchange.getResponseBody();
			os.write(response);
			os.close();
		});
	}

	private void deleteTempCertFiles() {
		Paths.get(System.getProperty("java.io.tmpdir"), "testclient.truststore").toFile().delete();
		Paths.get(System.getProperty("java.io.tmpdir"), "testclient.keystore").toFile().delete();
	}
}
