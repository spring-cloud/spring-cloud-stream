/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.DeprecatedConfigurationProperty;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 * @author Rafal Zukowski
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.binder")
public class KafkaBinderConfigurationProperties {

	private static final String DEFAULT_KAFKA_CONNECTION_STRING = "localhost:9092";

	private final Transaction transaction = new Transaction();

	private final KafkaProperties kafkaProperties;

	private String[] zkNodes = new String[] { "localhost" };

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

	private String defaultZkPort = "2181";

	private String[] brokers = new String[] { "localhost" };

	private String defaultBrokerPort = "9092";

	private String[] headers = new String[] {};

	private int offsetUpdateTimeWindow = 10000;

	private int offsetUpdateCount;

	private int offsetUpdateShutdownTimeout = 2000;

	private int maxWait = 100;

	private boolean autoCreateTopics = true;

	private boolean autoAddPartitions;

	private int socketBufferSize = 2097152;

	/**
	 * ZK session timeout in milliseconds.
	 */
	private int zkSessionTimeout = 10000;

	/**
	 * ZK Connection timeout in milliseconds.
	 */
	private int zkConnectionTimeout = 10000;

	private String requiredAcks = "1";

	private short replicationFactor = 1;

	private int fetchSize = 1024 * 1024;

	private int minPartitionCount = 1;

	private int queueSize = 8192;

	/**
	 * Time to wait to get partition information in seconds; default 60.
	 */
	private int healthTimeout = 60;

	private JaasLoginModuleConfiguration jaas;

	/**
	 * The bean name of a custom header mapper to use instead of a {@link org.springframework.kafka.support.DefaultKafkaHeaderMapper}.
	 */
	private String headerMapperBeanName;


	public KafkaBinderConfigurationProperties(KafkaProperties kafkaProperties) {
		Assert.notNull(kafkaProperties, "'kafkaProperties' cannot be null");
		this.kafkaProperties = kafkaProperties;
	}

	public Transaction getTransaction() {
		return this.transaction;
	}

	/**
	 * No longer used.
	 * @return the connection String
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	@Deprecated
	public String getZkConnectionString() {
		return toConnectionString(this.zkNodes, this.defaultZkPort);
	}

	public String getKafkaConnectionString() {
		return toConnectionString(this.brokers, this.defaultBrokerPort);
	}

	public String getDefaultKafkaConnectionString() {
		return DEFAULT_KAFKA_CONNECTION_STRING;
	}

	public String[] getHeaders() {
		return this.headers;
	}

	/**
	 * No longer used.
	 * @return the window.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getOffsetUpdateTimeWindow() {
		return this.offsetUpdateTimeWindow;
	}

	/**
	 * No longer used.
	 * @return the count.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getOffsetUpdateCount() {
		return this.offsetUpdateCount;
	}

	/**
	 * No longer used.
	 * @return the timeout.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getOffsetUpdateShutdownTimeout() {
		return this.offsetUpdateShutdownTimeout;
	}

	/**
	 * Zookeeper nodes.
	 * @return the nodes.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public String[] getZkNodes() {
		return this.zkNodes;
	}

	/**
	 * Zookeeper nodes.
	 * @param zkNodes the nodes.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public void setZkNodes(String... zkNodes) {
		this.zkNodes = zkNodes;
	}

	/**
	 * Zookeeper port.
	 * @param defaultZkPort the port.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public void setDefaultZkPort(String defaultZkPort) {
		this.defaultZkPort = defaultZkPort;
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
	 * No longer used.
	 * @param offsetUpdateTimeWindow the window.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setOffsetUpdateTimeWindow(int offsetUpdateTimeWindow) {
		this.offsetUpdateTimeWindow = offsetUpdateTimeWindow;
	}

	/**
	 * No longer used.
	 * @param offsetUpdateCount the count.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setOffsetUpdateCount(int offsetUpdateCount) {
		this.offsetUpdateCount = offsetUpdateCount;
	}

	/**
	 * No longer used.
	 * @param offsetUpdateShutdownTimeout the timeout.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setOffsetUpdateShutdownTimeout(int offsetUpdateShutdownTimeout) {
		this.offsetUpdateShutdownTimeout = offsetUpdateShutdownTimeout;
	}

	/**
	 * Zookeeper session timeout.
	 * @return the timeout.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public int getZkSessionTimeout() {
		return this.zkSessionTimeout;
	}

	/**
	 * Zookeeper session timeout.
	 * @param zkSessionTimeout the timout
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public void setZkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
	}

	/**
	 * Zookeeper connection timeout.
	 * @return the timout.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public int getZkConnectionTimeout() {
		return this.zkConnectionTimeout;
	}

	/**
	 * Zookeeper connection timeout.
	 * @param zkConnectionTimeout the timeout.
	 * @deprecated connection to zookeeper is no longer necessary
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "No longer necessary since 2.0")
	public void setZkConnectionTimeout(int zkConnectionTimeout) {
		this.zkConnectionTimeout = zkConnectionTimeout;
	}

	/**
	 * Converts an array of host values to a comma-separated String.
	 *
	 * It will append the default port value, if not already specified.
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

	/**
	 * No longer used.
	 * @return the wait.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getMaxWait() {
		return this.maxWait;
	}

	/**
	 * No longer user.
	 * @param maxWait the wait.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public String getRequiredAcks() {
		return this.requiredAcks;
	}

	public void setRequiredAcks(int requiredAcks) {
		this.requiredAcks = String.valueOf(requiredAcks);
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

	/**
	 * No longer used.
	 * @return the size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getFetchSize() {
		return this.fetchSize;
	}

	/**
	 * No longer used.
	 * @param fetchSize the size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
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

	/**
	 * No longer used.
	 * @return the queue size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public int getQueueSize() {
		return this.queueSize;
	}

	/**
	 * No longer used.
	 * @param queueSize the queue size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0")
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public boolean isAutoCreateTopics() {
		return this.autoCreateTopics;
	}

	public void setAutoCreateTopics(boolean autoCreateTopics) {
		this.autoCreateTopics = autoCreateTopics;
	}

	public boolean isAutoAddPartitions() {
		return this.autoAddPartitions;
	}

	public void setAutoAddPartitions(boolean autoAddPartitions) {
		this.autoAddPartitions = autoAddPartitions;
	}

	/**
	 * No longer used; set properties such as this via {@link #getConfiguration()
	 * configuration}.
	 * @return the size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0, set properties such as this via 'configuration'")
	public int getSocketBufferSize() {
		return this.socketBufferSize;
	}

	/**
	 * No longer used; set properties such as this via {@link #getConfiguration()
	 * configuration}.
	 * @param socketBufferSize the size.
	 * @deprecated
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.0, set properties such as this via 'configuration'")
	public void setSocketBufferSize(int socketBufferSize) {
		this.socketBufferSize = socketBufferSize;
	}

	public Map<String, String> getConfiguration() {
		return configuration;
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
		for (Map.Entry<String, String> configurationEntry : this.configuration.entrySet()) {
			if (ConsumerConfig.configNames().contains(configurationEntry.getKey())) {
				consumerConfiguration.put(configurationEntry.getKey(), configurationEntry.getValue());
			}
		}
		consumerConfiguration.putAll(this.consumerProperties);
		// Override Spring Boot bootstrap server setting if left to default with the value
		// configured in the binder
		if (ObjectUtils.isEmpty(consumerConfiguration.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			consumerConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectionString());
		}
		else {
			Object boostrapServersConfig = consumerConfiguration.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
			if (boostrapServersConfig instanceof List) {
				@SuppressWarnings("unchecked")
				List<String> bootStrapServers = (List<String>) consumerConfiguration
						.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
				if (bootStrapServers.size() == 1 && bootStrapServers.get(0).equals("localhost:9092")) {
					consumerConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectionString());
				}
			}
		}
		return Collections.unmodifiableMap(consumerConfiguration);
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
		for (Map.Entry<String, String> configurationEntry : configuration.entrySet()) {
			if (ProducerConfig.configNames().contains(configurationEntry.getKey())) {
				producerConfiguration.put(configurationEntry.getKey(), configurationEntry.getValue());
			}
		}
		producerConfiguration.putAll(this.producerProperties);
		// Override Spring Boot bootstrap server setting if left to default with the value
		// configured in the binder
		if (ObjectUtils.isEmpty(producerConfiguration.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectionString());
		}
		else {
			Object boostrapServersConfig = producerConfiguration.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
			if (boostrapServersConfig instanceof List) {
				@SuppressWarnings("unchecked")
				List<String> bootStrapServers = (List<String>) producerConfiguration
						.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
				if (bootStrapServers.size() == 1 && bootStrapServers.get(0).equals("localhost:9092")) {
					producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectionString());
				}
			}
		}
		return Collections.unmodifiableMap(producerConfiguration);
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

	public static class Transaction {

		private final KafkaProducerProperties producer = new KafkaProducerProperties();

		private String transactionIdPrefix;

		public String getTransactionIdPrefix() {
			return this.transactionIdPrefix;
		}

		public void setTransactionIdPrefix(String transactionIdPrefix) {
			this.transactionIdPrefix = transactionIdPrefix;
		}

		public KafkaProducerProperties getProducer() {
			return this.producer;
		}

	}

}
