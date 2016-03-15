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

package org.springframework.cloud.stream.binder.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder.Mode;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.kafka")
class KafkaBinderConfigurationProperties {

	private String[] zkNodes;

	private String defaultZkPort;

	private String[] brokers;

	private String defaultBrokerPort;

	private String[] headers;

	private KafkaMessageChannelBinder.Mode mode;

	private int offsetUpdateTimeWindow;

	private int offsetUpdateCount;

	private int offsetUpdateShutdownTimeout;

	private int maxWait = 100;
	/**
	 * ZK session timeout in milliseconds.
	 */
	private int zkSessionTimeout;

	/**
	 * ZK Connection timeout in milliseconds.
	 */
	private int zkConnectionTimeout;

	private int requiredAcks = 1;

	private int replicationFactor = 1;

	private int fetchSize = 1024 * 1024;

	private int minPartitionCount = 1;

	private int queueSize;

	public String getZkConnectionString() {
		return toConnectionString(this.zkNodes, this.defaultZkPort);
	}

	public String getKafkaConnectionString() {
		return toConnectionString(this.brokers, this.defaultBrokerPort);
	}

	public String[] getHeaders() {
		return headers;
	}

	public Mode getMode() {
		return this.mode;
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

	public void setZkNodes(String[] zkNodes) {
		this.zkNodes = zkNodes;
	}

	public void setDefaultZkPort(String defaultZkPort) {
		this.defaultZkPort = defaultZkPort;
	}

	public void setBrokers(String[] brokers) {
		this.brokers = brokers;
	}

	public void setDefaultBrokerPort(String defaultBrokerPort) {
		this.defaultBrokerPort = defaultBrokerPort;
	}


	public void setHeaders(String[] headers) {
		this.headers = headers;
	}

	public void setMode(KafkaMessageChannelBinder.Mode mode) {
		this.mode = mode;
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
		return maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getRequiredAcks() {
		return requiredAcks;
	}

	public void setRequiredAcks(int requiredAcks) {
		this.requiredAcks = requiredAcks;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public int getMinPartitionCount() {
		return minPartitionCount;
	}

	public void setMinPartitionCount(int minPartitionCount) {
		this.minPartitionCount = minPartitionCount;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

}
