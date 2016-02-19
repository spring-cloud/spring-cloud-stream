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
package org.springframework.cloud.stream.binder.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
@ConfigurationProperties(value = "spring.cloud.stream.binder.kafka.default")
public class KafkaBinderDefaultProperties {

	private int batchSize;

	private long batchTimeout;

	private int requiredAcks;

	private int replicationFactor;

	private int concurrency;

	private String compressionCodec;

	private boolean autoCommitEnabled;

	private int fetchSize;

	private int minPartitionCount;

	private int queueSize;

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public long getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(long batchTimeout) {
		this.batchTimeout = batchTimeout;
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

	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public boolean isAutoCommitEnabled() {
		return autoCommitEnabled;
	}

	public void setAutoCommitEnabled(boolean autoCommitEnabled) {
		this.autoCommitEnabled = autoCommitEnabled;
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
