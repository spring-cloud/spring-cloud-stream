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

import com.fasterxml.jackson.annotation.JsonGetter;
import jakarta.validation.constraints.NotNull;

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

	/**
	 * Upper limit, in bytes, of how much data the Kafka producer attempts to batch before sending.
	 */
	private int bufferSize = 16384;

	/**
	 * Set the compression.type producer property. Supported values are none, gzip, snappy and lz4.
	 * See {@link CompressionType} for more details.
	 */
	private CompressionType compressionType = CompressionType.none;

	/**
	 * Whether the producer is synchronous.
	 */
	private boolean sync;

	/**
	 * A SpEL expression evaluated against the outgoing message used to evaluate the time to wait
	 * for ack when synchronous publish is enabled.
	 */
	private Expression sendTimeoutExpression;

	/**
	 * How long the producer waits to allow more messages to accumulate in the same batch before sending the messages.
	 */
	private int batchTimeout;

	/**
	 * A SpEL expression evaluated against the outgoing message used to populate the key of the produced Kafka message.
	 */
	private Expression messageKeyExpression;

	/**
	 * A comma-delimited list of simple patterns to match Spring messaging headers
	 * to be mapped to the Kafka Headers in the ProducerRecord.
	 */
	private String[] headerPatterns;

	/**
	 * Map with a key/value pair containing generic Kafka producer properties.
	 */
	private Map<String, String> configuration = new HashMap<>();

	/**
	 * Various topic level properties. @see {@link KafkaTopicProperties} for more details.
	 */
	private KafkaTopicProperties topic = new KafkaTopicProperties();

	/**
	 * Set to true to override the default binding destination (topic name) with the value of the
	 * KafkaHeaders.TOPIC message header in the outbound message. If the header is not present,
	 * the default binding destination is used.
	 */
	private boolean useTopicHeader;

	/**
	 * The bean name of a MessageChannel to which successful send results should be sent;
	 * the bean must exist in the application context. When using the reactive binder,
	 * the channel must be of type FluxMessageChannel.
	 */
	private String recordMetadataChannel;

	/**
	 * Transaction manager bean name - overrides the binder's transaction configuration.
	 */
	private String transactionManager;

	/*
	 * Timeout value in seconds for the duration to wait when closing the producer.
	 * If not set this defaults to 30 seconds.
	 */
	private int closeTimeout;

	/**
	 * Set to true to disable transactions.
	 */
	private boolean allowNonTransactional;

	/**
	 * @return buffer size
	 *
	 * Upper limit, in bytes, of how much data the Kafka producer attempts to batch before sending.
	 */
	public int getBufferSize() {
		return this.bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	/**
	 * @return compression type {@link CompressionType}
	 *
	 * Set the compression.type producer property. Supported values are none, gzip, snappy, lz4 and zstd.
	 * See {@link CompressionType} for more details.
	 */
	@NotNull
	public CompressionType getCompressionType() {
		return this.compressionType;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	/**
	 * @return if synchronous sending is enabled
	 *
	 * Whether the producer is synchronous.
	 */
	public boolean isSync() {
		return this.sync;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	/**
	 * @return timeout expression for send
	 *
	 * A SpEL expression evaluated against the outgoing message used to evaluate the time to wait
	 * for ack when synchronous publish is enabled.
	 */
	public Expression getSendTimeoutExpression() {
		return this.sendTimeoutExpression;
	}

	public void setSendTimeoutExpression(Expression sendTimeoutExpression) {
		this.sendTimeoutExpression = sendTimeoutExpression;
	}

	/**
	 * @return batch timeout
	 *
	 * How long the producer waits to allow more messages to accumulate in the same batch before sending the messages.
	 */
	public int getBatchTimeout() {
		return this.batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	/**
	 * @return message key expression
	 *
	 * A SpEL expression evaluated against the outgoing message used to populate the key of the produced Kafka message.
	 */
	public Expression getMessageKeyExpression() {
		return this.messageKeyExpression;
	}

	@JsonGetter("messageKeyExpression")
	public String getTheMessageKeyExpression() {
		if (this.messageKeyExpression != null) {
			return this.messageKeyExpression.getExpressionString();
		}
		return null;
	}

	public void setMessageKeyExpression(Expression messageKeyExpression) {
		this.messageKeyExpression = messageKeyExpression;
	}

	/**
	 * @return header patterns
	 *
	 * A comma-delimited list of simple patterns to match Spring messaging headers
	 * to be mapped to the Kafka Headers in the ProducerRecord.
	 */
	public String[] getHeaderPatterns() {
		return this.headerPatterns;
	}

	public void setHeaderPatterns(String[] headerPatterns) {
		this.headerPatterns = headerPatterns;
	}

	/**
	 * @return map of configuration
	 *
	 * Map with a key/value pair containing generic Kafka producer properties.
	 */
	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	/**
	 * @return topic properties
	 *
	 * Various topic level properties. @see {@link KafkaTopicProperties} for more details.
	 */
	public KafkaTopicProperties getTopic() {
		return this.topic;
	}

	public void setTopic(KafkaTopicProperties topic) {
		this.topic = topic;
	}

	/**
	 * @return if using topic header
	 *
	 * Set to true to override the default binding destination (topic name) with the value of the
	 * KafkaHeaders.TOPIC message header in the outbound message. If the header is not present,
	 * the default binding destination is used.
	 */
	public boolean isUseTopicHeader() {
		return this.useTopicHeader;
	}

	public void setUseTopicHeader(boolean useTopicHeader) {
		this.useTopicHeader = useTopicHeader;
	}

	/**
	 * @return record metadata channel
	 *
	 * The bean name of a MessageChannel to which successful send results should be sent;
	 * the bean must exist in the application context.
	 */
	public String getRecordMetadataChannel() {
		return this.recordMetadataChannel;
	}

	public void setRecordMetadataChannel(String recordMetadataChannel) {
		this.recordMetadataChannel = recordMetadataChannel;
	}

	/**
	 * @return the transaction manager bean name.
	 *
	 * Transaction manager bean name (must be {@code KafkaAwareTransactionManager}.
	 */
	public String getTransactionManager() {
		return this.transactionManager;
	}

	public void setTransactionManager(String transactionManager) {
		this.transactionManager = transactionManager;
	}

	/*
	 * @return timeout in seconds for closing the producer
	 */
	public int getCloseTimeout() {
		return this.closeTimeout;
	}

	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	public boolean isAllowNonTransactional() {
		return this.allowNonTransactional;
	}

	public void setAllowNonTransactional(boolean allowNonTransactional) {
		this.allowNonTransactional = allowNonTransactional;
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

		/**
		 * zstd compression.
		 */
		zstd,

	}

}
