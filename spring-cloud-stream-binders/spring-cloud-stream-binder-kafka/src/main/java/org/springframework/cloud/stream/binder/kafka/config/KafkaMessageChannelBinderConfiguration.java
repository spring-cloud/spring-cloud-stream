/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.kafka.support.ZookeeperConnect;

/**
 * @author David Turanski
 */
@Configuration
@EnableConfigurationProperties(KafkaBinderConfigurationProperties.class)
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.kafka")
public class KafkaMessageChannelBinderConfiguration {

	private String zkAddress;

	private String brokers;

	private KafkaMessageChannelBinder.Mode mode;

	private String offsetStoreTopic;

	private int offsetStoreSegmentSize;

	private int offsetStoreRetentionTime;

	private int offsetStoreRequiredAcks;

	private int offsetStoreMaxFetchSize;

	private int offsetStoreBatchBytes;

	private int offsetStoreBatchTime;

	private int offsetUpdateTimeWindow;

	private int offsetUpdateCount;

	private int offsetUpdateShutdownTimeout;

	@Autowired
	private Codec codec;

	@Autowired
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Bean
	ZookeeperConnect zookeeperConnect() {
		ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
		zookeeperConnect.setZkConnect(zkAddress);
		return zookeeperConnect;
	}

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder() {
		KafkaMessageChannelBinder kafkaMessageChannelBinder = new KafkaMessageChannelBinder(zookeeperConnect(),
				brokers, zkAddress, new String[0]);
		kafkaMessageChannelBinder.setCodec(codec);
		kafkaMessageChannelBinder.setMode(mode);
		kafkaMessageChannelBinder.setOffsetStoreTopic(offsetStoreTopic);
		kafkaMessageChannelBinder.setOffsetStoreSegmentSize(offsetStoreSegmentSize);
		kafkaMessageChannelBinder.setOffsetStoreRetentionTime(offsetStoreRetentionTime);
		kafkaMessageChannelBinder.setOffsetStoreRequiredAcks(offsetStoreRequiredAcks);
		kafkaMessageChannelBinder.setOffsetStoreMaxFetchSize(offsetStoreMaxFetchSize);
		kafkaMessageChannelBinder.setOffsetStoreBatchBytes(offsetStoreBatchBytes);
		kafkaMessageChannelBinder.setOffsetStoreBatchTime(offsetStoreBatchTime);
		kafkaMessageChannelBinder.setOffsetUpdateTimeWindow(offsetUpdateTimeWindow);
		kafkaMessageChannelBinder.setOffsetUpdateCount(offsetUpdateCount);
		kafkaMessageChannelBinder.setOffsetUpdateShutdownTimeout(offsetUpdateShutdownTimeout);

		kafkaMessageChannelBinder.setDefaultAutoCommitEnabled(kafkaBinderConfigurationProperties.isAutoCommitEnabled());
		kafkaMessageChannelBinder.setDefaultBatchSize(kafkaBinderConfigurationProperties.getBatchSize());
		kafkaMessageChannelBinder.setDefaultBatchTimeout(kafkaBinderConfigurationProperties.getBatchTimeout());
		kafkaMessageChannelBinder.setDefaultCompressionCodec(kafkaBinderConfigurationProperties
				.getCompressionCodec());
		kafkaMessageChannelBinder.setDefaultConcurrency(kafkaBinderConfigurationProperties.getConcurrency());
		kafkaMessageChannelBinder.setDefaultFetchSize(kafkaBinderConfigurationProperties.getFetchSize());
		kafkaMessageChannelBinder.setDefaultMinPartitionCount(kafkaBinderConfigurationProperties
				.getMinPartitionCount());
		kafkaMessageChannelBinder.setDefaultQueueSize(kafkaBinderConfigurationProperties.getQueueSize());
		kafkaMessageChannelBinder.setDefaultReplicationFactor(kafkaBinderConfigurationProperties
				.getReplicationFactor());
		kafkaMessageChannelBinder.setDefaultRequiredAcks(kafkaBinderConfigurationProperties.getRequiredAcks());

		return kafkaMessageChannelBinder;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}

	public void setMode(KafkaMessageChannelBinder.Mode mode) {
		this.mode = mode;
	}

	public void setOffsetStoreTopic(String offsetStoreTopic) {
		this.offsetStoreTopic = offsetStoreTopic;
	}

	public void setOffsetStoreSegmentSize(int offsetStoreSegmentSize) {
		this.offsetStoreSegmentSize = offsetStoreSegmentSize;
	}

	public void setOffsetStoreRetentionTime(int offsetStoreRetentionTime) {
		this.offsetStoreRetentionTime = offsetStoreRetentionTime;
	}

	public void setOffsetStoreRequiredAcks(int offsetStoreRequiredAcks) {
		this.offsetStoreRequiredAcks = offsetStoreRequiredAcks;
	}

	public void setOffsetStoreMaxFetchSize(int offsetStoreMaxFetchSize) {
		this.offsetStoreMaxFetchSize = offsetStoreMaxFetchSize;
	}

	public void setOffsetStoreBatchBytes(int offsetStoreBatchBytes) {
		this.offsetStoreBatchBytes = offsetStoreBatchBytes;
	}

	public void setOffsetStoreBatchTime(int offsetStoreBatchTime) {
		this.offsetStoreBatchTime = offsetStoreBatchTime;
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

	public void setCodec(Codec codec) {
		this.codec = codec;
	}

}
