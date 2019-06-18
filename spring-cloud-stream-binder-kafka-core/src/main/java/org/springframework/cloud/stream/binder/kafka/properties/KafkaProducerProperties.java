/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.DeprecatedConfigurationProperty;
import org.springframework.expression.Expression;

/**
 * Extended producer properties for Kafka binder.
 *
 * @author Marius Bogoevici
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Aldo Sinanaj
 */
public class KafkaProducerProperties {

	private int bufferSize = 16384;

	private CompressionType compressionType = CompressionType.none;

	private boolean sync;

	private int batchTimeout;

	private Expression messageKeyExpression;

	private String[] headerPatterns;

	private Map<String, String> configuration = new HashMap<>();

	private KafkaTopicProperties topic = new KafkaTopicProperties();

	private boolean useTopicHeader;

	public int getBufferSize() {
		return this.bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	@NotNull
	public CompressionType getCompressionType() {
		return this.compressionType;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	public boolean isSync() {
		return this.sync;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	public int getBatchTimeout() {
		return this.batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public Expression getMessageKeyExpression() {
		return this.messageKeyExpression;
	}

	public void setMessageKeyExpression(Expression messageKeyExpression) {
		this.messageKeyExpression = messageKeyExpression;
	}

	public String[] getHeaderPatterns() {
		return this.headerPatterns;
	}

	public void setHeaderPatterns(String[] headerPatterns) {
		this.headerPatterns = headerPatterns;
	}

	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	/**
	 * No longer used; get properties such as this via {@link #getTopic()}.
	 * @return Kafka admin properties
	 * @deprecated No longer used
	 */
	@Deprecated
	@DeprecatedConfigurationProperty(reason = "Not used since 2.1.1, set properties such as this via 'topic'")
	@SuppressWarnings("deprecation")
	public KafkaAdminProperties getAdmin() {
		// Temporary workaround to copy the topic properties to the admin one.
		final KafkaAdminProperties kafkaAdminProperties = new KafkaAdminProperties();
		kafkaAdminProperties.setReplicationFactor(this.topic.getReplicationFactor());
		kafkaAdminProperties.setReplicasAssignments(this.topic.getReplicasAssignments());
		kafkaAdminProperties.setConfiguration(this.topic.getProperties());
		return kafkaAdminProperties;
	}

	@Deprecated
	@SuppressWarnings("deprecation")
	public void setAdmin(KafkaAdminProperties admin) {
		this.topic = admin;
	}

	public KafkaTopicProperties getTopic() {
		return this.topic;
	}

	public void setTopic(KafkaTopicProperties topic) {
		this.topic = topic;
	}

	public boolean isUseTopicHeader() {
		return this.useTopicHeader;
	}

	public void setUseTopicHeader(boolean useTopicHeader) {
		this.useTopicHeader = useTopicHeader;
	}

	/**
	 * Enumeration for compression types.
	 */
	public enum CompressionType {

		/**
		 * No compression.
		 */
		none,

		/**
		 * gzip based compression.
		 */
		gzip,

		/**
		 * snappy based compression.
		 */
		snappy,

		/**
		 * lz4 compression.
		 */
		lz4,

		// /** // TODO: uncomment and fix docs when kafka-clients 2.1.0 or newer is the
		// default
		// * zstd compression
		// */
		// zstd

	}

}
