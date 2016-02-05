/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.kafka.support.LoggingProducerListener;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Soby Chacko
 */
@Configuration
@Import({KryoCodecAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class})
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.kafka")
public class KafkaMessageChannelBinderConfiguration {

	private String[] zkNodes;

	private String defaultZkPort;

	private String[] brokers;

	private String defaultBrokerPort;

	private String[] headers;

	private KafkaMessageChannelBinder.Mode mode;

	private int offsetUpdateTimeWindow;

	private int offsetUpdateCount;

	private int offsetUpdateShutdownTimeout;

	private boolean resetOffsets = false;

	private KafkaMessageChannelBinder.StartOffset startOffset;

	@Autowired
	private Codec codec;

	@Autowired
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Autowired
	private ProducerListener producerListener;

	@Bean
	ZookeeperConnect zookeeperConnect() {
		ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
		zookeeperConnect.setZkConnect(getZkConnectionString());
		return zookeeperConnect;
	}

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder() {
		KafkaMessageChannelBinder kafkaMessageChannelBinder = ObjectUtils.isEmpty(headers) ?
				new KafkaMessageChannelBinder(zookeeperConnect(), getKafkaConnectionString(), getZkConnectionString())
				: new KafkaMessageChannelBinder(zookeeperConnect(), getKafkaConnectionString(), getZkConnectionString(),
						headers);
		kafkaMessageChannelBinder.setCodec(codec);
		kafkaMessageChannelBinder.setMode(mode);
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

		kafkaMessageChannelBinder.setResetOffsets(resetOffsets);
		kafkaMessageChannelBinder.setStartOffset(startOffset);

		kafkaMessageChannelBinder.setProducerListener(producerListener);

		return kafkaMessageChannelBinder;
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

	public String[] getHeaders() {
		return headers;
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

	public void setCodec(Codec codec) {
		this.codec = codec;
	}

	public String getZkConnectionString() {
		return toConnectionString(this.zkNodes, this.defaultZkPort);
	}

	public String getKafkaConnectionString() {
		return toConnectionString(this.brokers, this.defaultBrokerPort);
	}

	public KafkaMessageChannelBinder.StartOffset getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(KafkaMessageChannelBinder.StartOffset startOffset) {
		this.startOffset = startOffset;
	}

	public boolean isResetOffsets() {
		return resetOffsets;
	}

	public void setResetOffsets(boolean resetOffsets) {
		this.resetOffsets = resetOffsets;
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
}
