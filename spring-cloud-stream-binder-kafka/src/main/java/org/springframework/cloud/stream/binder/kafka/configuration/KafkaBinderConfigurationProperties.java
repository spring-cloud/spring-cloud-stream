/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.binder")
public class KafkaBinderConfigurationProperties {

	private String[] zkNodes = new String[] {"localhost"};

	private Map<String, String> configuration = new HashMap<>();

	private String defaultZkPort = "2181";

	private String[] brokers = new String[] {"localhost"};

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

	private int requiredAcks = 1;

	private int replicationFactor = 1;

	private int fetchSize = 1024 * 1024;

	private int minPartitionCount = 1;

	private int queueSize = 8192;

	public String getZkConnectionString() {
		return toConnectionString(this.zkNodes, this.defaultZkPort);
	}

	public String getKafkaConnectionString() {
		return toConnectionString(this.brokers, this.defaultBrokerPort);
	}

	public String[] getHeaders() {
		return this.headers;
	}

	public int getOffsetUpdateTimeWindow() {
		return this.offsetUpdateTimeWindow;
	}

	public int getOffsetUpdateCount() {
		return this.offsetUpdateCount;
	}

	public int getOffsetUpdateShutdownTimeout() {
		return this.offsetUpdateShutdownTimeout;
	}

	public String[] getZkNodes() {
		return this.zkNodes;
	}

	public void setZkNodes(String... zkNodes) {
		this.zkNodes = zkNodes;
	}

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

	public void setOffsetUpdateTimeWindow(int offsetUpdateTimeWindow) {
		this.offsetUpdateTimeWindow = offsetUpdateTimeWindow;
	}

	public void setOffsetUpdateCount(int offsetUpdateCount) {
		this.offsetUpdateCount = offsetUpdateCount;
	}

	public void setOffsetUpdateShutdownTimeout(int offsetUpdateShutdownTimeout) {
		this.offsetUpdateShutdownTimeout = offsetUpdateShutdownTimeout;
	}

	public int getZkSessionTimeout() {
		return this.zkSessionTimeout;
	}

	public void setZkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
	}

	public int getZkConnectionTimeout() {
		return this.zkConnectionTimeout;
	}

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

	public int getMaxWait() {
		return this.maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getRequiredAcks() {
		return this.requiredAcks;
	}

	public void setRequiredAcks(int requiredAcks) {
		this.requiredAcks = requiredAcks;
	}

	public int getReplicationFactor() {
		return this.replicationFactor;
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getFetchSize() {
		return this.fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public int getMinPartitionCount() {
		return this.minPartitionCount;
	}

	public void setMinPartitionCount(int minPartitionCount) {
		this.minPartitionCount = minPartitionCount;
	}

	public int getQueueSize() {
		return this.queueSize;
	}

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

	public int getSocketBufferSize() {
		return this.socketBufferSize;
	}

	public void setSocketBufferSize(int socketBufferSize) {
		this.socketBufferSize = socketBufferSize;
	}

	public Map<String, String> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}
}
