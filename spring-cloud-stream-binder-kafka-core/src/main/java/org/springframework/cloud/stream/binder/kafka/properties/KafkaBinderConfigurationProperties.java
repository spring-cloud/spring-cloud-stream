/*
 * Copyright 2015-2018 the original author or authors.
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties.CompressionType;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration properties for the Kafka binder. The properties in this class are
 * prefixed with <b>spring.cloud.stream.kafka.binder</b>.
 *
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 * @author Rafal Zukowski
 * @author Aldo Sinanaj
 * @author Lukasz Kaminski
 * @author Chukwubuikem Ume-Ugwa
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.binder")
public class KafkaBinderConfigurationProperties {

	private static final String DEFAULT_KAFKA_CONNECTION_STRING = "localhost:9092";

	private final Log logger = LogFactory.getLog(getClass());

	private final Transaction transaction = new Transaction();

	private final KafkaProperties kafkaProperties;

	/**
	 * Arbitrary kafka properties that apply to both producers and consumers.
	 */
	private Map<String, String> configuration = new HashMap<>();

	/**
	 * Arbitrary kafka consumer properties.
	 */
	private Map<String, String> consumerProperties = new HashMap<>();

	/**
	 * Arbitrary kafka producer properties.
	 */
	private Map<String, String> producerProperties = new HashMap<>();

	private String[] brokers = new String[] { "localhost" };

	private String defaultBrokerPort = "9092";

	private String[] headers = new String[] {};

	private boolean autoCreateTopics = true;

	private boolean autoAlterTopics;

	private boolean autoAddPartitions;

	private boolean considerDownWhenAnyPartitionHasNoLeader;

	private String requiredAcks = "1";

	private short replicationFactor = -1;

	private int minPartitionCount = 1;

	/**
	 * Time to wait to get partition information in seconds; default 60.
	 */
	private int healthTimeout = 60;

	private JaasLoginModuleConfiguration jaas;

	/**
	 * The bean name of a custom header mapper to use instead of a
	 * {@link org.springframework.kafka.support.DefaultKafkaHeaderMapper}.
	 */
	private String headerMapperBeanName;

	/**
	 * Time between retries after AuthorizationException is caught in
	 * the ListenerContainer; defalt is null which disables retries.
	 * For more info see: {@link org.springframework.kafka.listener.ConsumerProperties#setAuthorizationExceptionRetryInterval(java.time.Duration)}
	 */
	private Duration authorizationExceptionRetryInterval;

	/**
	 * When a certificate store location is given as classpath URL (classpath:), then the binder
	 * moves the resource from the classpath location inside the JAR to a location on
	 * the filesystem. If this value is set, then this location is used, otherwise, the
	 * certificate file is copied to the directory returned by java.io.tmpdir.
	 */
	private String certificateStoreDirectory;

	public KafkaBinderConfigurationProperties(KafkaProperties kafkaProperties) {
		Assert.notNull(kafkaProperties, "'kafkaProperties' cannot be null");
		this.kafkaProperties = kafkaProperties;
	}

	public KafkaProperties getKafkaProperties() {
		return this.kafkaProperties;
	}

	public Transaction getTransaction() {
		return this.transaction;
	}

	public String getKafkaConnectionString() {
		// We need to do a check on certificate file locations to see if they are given as classpath resources.
		// If that is the case, then we will move them to a file system location and use those as the certificate locations.
		// This is due to a limitation in Kafka itself in which it doesn't allow reading certificate resources from the classpath.
		// See this: https://issues.apache.org/jira/browse/KAFKA-7685
		// and this: https://cwiki.apache.org/confluence/display/KAFKA/KIP-398%3A+Support+reading+trust+store+from+classpath
		moveCertsToFileSystemIfNecessary();

		return toConnectionString(this.brokers, this.defaultBrokerPort);
	}

	private void moveCertsToFileSystemIfNecessary() {
		try {
			final String trustStoreLocation = this.configuration.get("ssl.truststore.location");
			if (trustStoreLocation != null && trustStoreLocation.startsWith("classpath:")) {
				final String fileSystemLocation = moveCertToFileSystem(trustStoreLocation, this.certificateStoreDirectory);
				// Overriding the value with absolute filesystem path.
				this.configuration.put("ssl.truststore.location", fileSystemLocation);
			}
			final String keyStoreLocation = this.configuration.get("ssl.keystore.location");
			if (keyStoreLocation != null && keyStoreLocation.startsWith("classpath:")) {
				final String fileSystemLocation = moveCertToFileSystem(keyStoreLocation, this.certificateStoreDirectory);
				// Overriding the value with absolute filesystem path.
				this.configuration.put("ssl.keystore.location", fileSystemLocation);
			}
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private String moveCertToFileSystem(String classpathLocation, String fileSystemLocation) throws IOException {
		File targetFile;
		final String tempDir = System.getProperty("java.io.tmpdir");
		Resource resource = new DefaultResourceLoader().getResource(classpathLocation);
		if (StringUtils.hasText(fileSystemLocation)) {
			final Path path = Paths.get(fileSystemLocation);
			if (!Files.exists(path) || !Files.isDirectory(path) || !Files.isWritable(path)) {
				logger.warn("The filesystem location to move the cert files (" + fileSystemLocation + ") " +
						"is not found or a directory that is writable. The system temp folder (java.io.tmpdir) will be used instead.");
				targetFile = new File(Paths.get(tempDir, resource.getFilename()).toString());
			}
			else {
				// the given location is verified to be a writable directory.
				targetFile = new File(Paths.get(fileSystemLocation, resource.getFilename()).toString());
			}
		}
		else {
			targetFile = new File(Paths.get(tempDir, resource.getFilename()).toString());
		}

		try (InputStream inputStream = resource.getInputStream()) {
			Files.copy(inputStream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
		return targetFile.getAbsolutePath();
	}

	public String getDefaultKafkaConnectionString() {
		return DEFAULT_KAFKA_CONNECTION_STRING;
	}

	public String[] getHeaders() {
		return this.headers;
	}

	public String[] getBrokers() {
		return this.brokers;
	}

	public void setBrokers(String... brokers) {
		this.brokers = brokers;
	}

	public void setDefaultBrokerPort(String defaultBrokerPort) {
		this.defaultBrokerPort = defaultBrokerPort;
	}

	public void setHeaders(String... headers) {
		this.headers = headers;
	}

	/**
	 * Converts an array of host values to a comma-separated String. It will append the
	 * default port value, if not already specified.
	 * @param hosts host string
	 * @param defaultPort port
	 * @return formatted connection string
	 */
	private String toConnectionString(String[] hosts, String defaultPort) {
		String[] fullyFormattedHosts = new String[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			if (hosts[i].contains(":") || StringUtils.isEmpty(defaultPort)) {
				fullyFormattedHosts[i] = hosts[i];
			}
			else {
				fullyFormattedHosts[i] = hosts[i] + ":" + defaultPort;
			}
		}
		return StringUtils.arrayToCommaDelimitedString(fullyFormattedHosts);
	}

	public String getRequiredAcks() {
		return this.requiredAcks;
	}

	public void setRequiredAcks(String requiredAcks) {
		this.requiredAcks = requiredAcks;
	}

	public short getReplicationFactor() {
		return this.replicationFactor;
	}

	public void setReplicationFactor(short replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getMinPartitionCount() {
		return this.minPartitionCount;
	}

	public void setMinPartitionCount(int minPartitionCount) {
		this.minPartitionCount = minPartitionCount;
	}

	public int getHealthTimeout() {
		return this.healthTimeout;
	}

	public void setHealthTimeout(int healthTimeout) {
		this.healthTimeout = healthTimeout;
	}

	public boolean isAutoCreateTopics() {
		return this.autoCreateTopics;
	}

	public void setAutoCreateTopics(boolean autoCreateTopics) {
		this.autoCreateTopics = autoCreateTopics;
	}

	public boolean isAutoAlterTopics() {
		return autoAlterTopics;
	}

	public void setAutoAlterTopics(boolean autoAlterTopics) {
		this.autoAlterTopics = autoAlterTopics;
	}

	public boolean isAutoAddPartitions() {
		return this.autoAddPartitions;
	}

	public void setAutoAddPartitions(boolean autoAddPartitions) {
		this.autoAddPartitions = autoAddPartitions;
	}

	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	public Map<String, String> getConsumerProperties() {
		return this.consumerProperties;
	}

	public void setConsumerProperties(Map<String, String> consumerProperties) {
		Assert.notNull(consumerProperties, "'consumerProperties' cannot be null");
		this.consumerProperties = consumerProperties;
	}

	public Map<String, String> getProducerProperties() {
		return this.producerProperties;
	}

	public void setProducerProperties(Map<String, String> producerProperties) {
		Assert.notNull(producerProperties, "'producerProperties' cannot be null");
		this.producerProperties = producerProperties;
	}

	/**
	 * Merge boot consumer properties, general properties from
	 * {@link #setConfiguration(Map)} that apply to consumers, properties from
	 * {@link #setConsumerProperties(Map)}, in that order.
	 * @return the merged properties.
	 */
	public Map<String, Object> mergedConsumerConfiguration() {
		Map<String, Object> consumerConfiguration = new HashMap<>();
		consumerConfiguration.putAll(this.kafkaProperties.buildConsumerProperties());
		// Copy configured binder properties that apply to consumers
		for (Map.Entry<String, String> configurationEntry : this.configuration
				.entrySet()) {
			if (ConsumerConfig.configNames().contains(configurationEntry.getKey())) {
				consumerConfiguration.put(configurationEntry.getKey(),
						configurationEntry.getValue());
			}
		}
		consumerConfiguration.putAll(this.consumerProperties);
		filterStreamManagedConfiguration(consumerConfiguration);
		// Override Spring Boot bootstrap server setting if left to default with the value
		// configured in the binder
		return getConfigurationWithBootstrapServer(consumerConfiguration,
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
	}

	/**
	 * Merge boot producer properties, general properties from
	 * {@link #setConfiguration(Map)} that apply to producers, properties from
	 * {@link #setProducerProperties(Map)}, in that order.
	 * @return the merged properties.
	 */
	public Map<String, Object> mergedProducerConfiguration() {
		Map<String, Object> producerConfiguration = new HashMap<>();
		producerConfiguration.putAll(this.kafkaProperties.buildProducerProperties());
		// Copy configured binder properties that apply to producers
		for (Map.Entry<String, String> configurationEntry : this.configuration
				.entrySet()) {
			if (ProducerConfig.configNames().contains(configurationEntry.getKey())) {
				producerConfiguration.put(configurationEntry.getKey(),
						configurationEntry.getValue());
			}
		}
		producerConfiguration.putAll(this.producerProperties);
		// Override Spring Boot bootstrap server setting if left to default with the value
		// configured in the binder
		return getConfigurationWithBootstrapServer(producerConfiguration,
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
	}

	private void filterStreamManagedConfiguration(Map<String, Object> configuration) {
		if (configuration.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
				&& configuration.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equals(true)) {
			logger.warn(constructIgnoredConfigMessage(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) +
					ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "=true is not supported by the Kafka binder");
			configuration.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		}
		if (configuration.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
			logger.warn(constructIgnoredConfigMessage(ConsumerConfig.GROUP_ID_CONFIG) +
					"Use spring.cloud.stream.default.group or spring.cloud.stream.binding.<name>.group to specify " +
					"the group instead of " + ConsumerConfig.GROUP_ID_CONFIG);
			configuration.remove(ConsumerConfig.GROUP_ID_CONFIG);
		}
	}

	private String constructIgnoredConfigMessage(String config) {
		return String.format("Ignoring provided value(s) for '%s'. ", config);
	}

	private Map<String, Object> getConfigurationWithBootstrapServer(
			Map<String, Object> configuration, String bootstrapServersConfig) {
		final String kafkaConnectionString = getKafkaConnectionString();
		if (ObjectUtils.isEmpty(configuration.get(bootstrapServersConfig)) ||
				!kafkaConnectionString.equals("localhost:9092")) {
			configuration.put(bootstrapServersConfig, kafkaConnectionString);
		}
		return Collections.unmodifiableMap(configuration);
	}

	public JaasLoginModuleConfiguration getJaas() {
		return this.jaas;
	}

	public void setJaas(JaasLoginModuleConfiguration jaas) {
		this.jaas = jaas;
	}

	public String getHeaderMapperBeanName() {
		return this.headerMapperBeanName;
	}

	public void setHeaderMapperBeanName(String headerMapperBeanName) {
		this.headerMapperBeanName = headerMapperBeanName;
	}

	public Duration getAuthorizationExceptionRetryInterval() {
		return authorizationExceptionRetryInterval;
	}

	public void setAuthorizationExceptionRetryInterval(Duration authorizationExceptionRetryInterval) {
		this.authorizationExceptionRetryInterval = authorizationExceptionRetryInterval;
	}

	public boolean isConsiderDownWhenAnyPartitionHasNoLeader() {
		return this.considerDownWhenAnyPartitionHasNoLeader;
	}

	public void setConsiderDownWhenAnyPartitionHasNoLeader(boolean considerDownWhenAnyPartitionHasNoLeader) {
		this.considerDownWhenAnyPartitionHasNoLeader = considerDownWhenAnyPartitionHasNoLeader;
	}

	public String getCertificateStoreDirectory() {
		return this.certificateStoreDirectory;
	}

	public void setCertificateStoreDirectory(String certificateStoreDirectory) {
		this.certificateStoreDirectory = certificateStoreDirectory;
	}

	/**
	 * Domain class that models transaction capabilities in Kafka.
	 */
	public static class Transaction {

		private final CombinedProducerProperties producer = new CombinedProducerProperties();

		private String transactionIdPrefix;

		public String getTransactionIdPrefix() {
			return this.transactionIdPrefix;
		}

		public void setTransactionIdPrefix(String transactionIdPrefix) {
			this.transactionIdPrefix = transactionIdPrefix;
		}

		public CombinedProducerProperties getProducer() {
			return this.producer;
		}

	}

	/**
	 * An combination of {@link ProducerProperties} and {@link KafkaProducerProperties} so
	 * that common and kafka-specific properties can be set for the transactional
	 * producer.
	 *
	 * @since 2.1
	 */
	public static class CombinedProducerProperties {

		private final ProducerProperties producerProperties = new ProducerProperties();

		private final KafkaProducerProperties kafkaProducerProperties = new KafkaProducerProperties();

		public Expression getPartitionKeyExpression() {
			return this.producerProperties.getPartitionKeyExpression();
		}

		public void setPartitionKeyExpression(Expression partitionKeyExpression) {
			this.producerProperties.setPartitionKeyExpression(partitionKeyExpression);
		}

		public boolean isPartitioned() {
			return this.producerProperties.isPartitioned();
		}

		public Expression getPartitionSelectorExpression() {
			return this.producerProperties.getPartitionSelectorExpression();
		}

		public void setPartitionSelectorExpression(
				Expression partitionSelectorExpression) {
			this.producerProperties
					.setPartitionSelectorExpression(partitionSelectorExpression);
		}

		public @Min(value = 1, message = "Partition count should be greater than zero.") int getPartitionCount() {
			return this.producerProperties.getPartitionCount();
		}

		public void setPartitionCount(int partitionCount) {
			this.producerProperties.setPartitionCount(partitionCount);
		}

		public String[] getRequiredGroups() {
			return this.producerProperties.getRequiredGroups();
		}

		public void setRequiredGroups(String... requiredGroups) {
			this.producerProperties.setRequiredGroups(requiredGroups);
		}

		public @AssertTrue(message = "Partition key expression and partition key extractor class properties "
				+ "are mutually exclusive.") boolean isValidPartitionKeyProperty() {
			return this.producerProperties.isValidPartitionKeyProperty();
		}

		public @AssertTrue(message = "Partition selector class and partition selector expression "
				+ "properties are mutually exclusive.") boolean isValidPartitionSelectorProperty() {
			return this.producerProperties.isValidPartitionSelectorProperty();
		}

		public HeaderMode getHeaderMode() {
			return this.producerProperties.getHeaderMode();
		}

		public void setHeaderMode(HeaderMode headerMode) {
			this.producerProperties.setHeaderMode(headerMode);
		}

		public boolean isUseNativeEncoding() {
			return this.producerProperties.isUseNativeEncoding();
		}

		public void setUseNativeEncoding(boolean useNativeEncoding) {
			this.producerProperties.setUseNativeEncoding(useNativeEncoding);
		}

		public boolean isErrorChannelEnabled() {
			return this.producerProperties.isErrorChannelEnabled();
		}

		public void setErrorChannelEnabled(boolean errorChannelEnabled) {
			this.producerProperties.setErrorChannelEnabled(errorChannelEnabled);
		}

		public String getPartitionKeyExtractorName() {
			return this.producerProperties.getPartitionKeyExtractorName();
		}

		public void setPartitionKeyExtractorName(String partitionKeyExtractorName) {
			this.producerProperties
					.setPartitionKeyExtractorName(partitionKeyExtractorName);
		}

		public String getPartitionSelectorName() {
			return this.producerProperties.getPartitionSelectorName();
		}

		public void setPartitionSelectorName(String partitionSelectorName) {
			this.producerProperties.setPartitionSelectorName(partitionSelectorName);
		}

		public int getBufferSize() {
			return this.kafkaProducerProperties.getBufferSize();
		}

		public void setBufferSize(int bufferSize) {
			this.kafkaProducerProperties.setBufferSize(bufferSize);
		}

		public @NotNull CompressionType getCompressionType() {
			return this.kafkaProducerProperties.getCompressionType();
		}

		public void setCompressionType(CompressionType compressionType) {
			this.kafkaProducerProperties.setCompressionType(compressionType);
		}

		public boolean isSync() {
			return this.kafkaProducerProperties.isSync();
		}

		public void setSync(boolean sync) {
			this.kafkaProducerProperties.setSync(sync);
		}

		public int getBatchTimeout() {
			return this.kafkaProducerProperties.getBatchTimeout();
		}

		public void setBatchTimeout(int batchTimeout) {
			this.kafkaProducerProperties.setBatchTimeout(batchTimeout);
		}

		public Expression getMessageKeyExpression() {
			return this.kafkaProducerProperties.getMessageKeyExpression();
		}

		public void setMessageKeyExpression(Expression messageKeyExpression) {
			this.kafkaProducerProperties.setMessageKeyExpression(messageKeyExpression);
		}

		public String[] getHeaderPatterns() {
			return this.kafkaProducerProperties.getHeaderPatterns();
		}

		public void setHeaderPatterns(String[] headerPatterns) {
			this.kafkaProducerProperties.setHeaderPatterns(headerPatterns);
		}

		public Map<String, String> getConfiguration() {
			return this.kafkaProducerProperties.getConfiguration();
		}

		public void setConfiguration(Map<String, String> configuration) {
			this.kafkaProducerProperties.setConfiguration(configuration);
		}

		public KafkaTopicProperties getTopic() {
			return this.kafkaProducerProperties.getTopic();
		}

		public void setTopic(KafkaTopicProperties topic) {
			this.kafkaProducerProperties.setTopic(topic);
		}

		public KafkaProducerProperties getExtension() {
			return this.kafkaProducerProperties;
		}

	}

}
