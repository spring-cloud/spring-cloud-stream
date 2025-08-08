/*
 * Copyright 2017-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.DeserializationExceptionHandler;

/**
 * Extended properties for Kafka Streams consumer.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gihong Park
 */
public class KafkaStreamsConsumerProperties extends KafkaConsumerProperties {

	private String applicationId;

	/**
	 * Key serde specified per binding.
	 */
	private String keySerde;

	/**
	 * Value serde specified per binding.
	 */
	private String valueSerde;

	/**
	 * Materialized as a KeyValueStore.
	 */
	private String materializedAs;

	/**
	 * Disable caching for materialized KTable.
	 */
	private boolean cachingDisabled;

	/**
	 * Disable logging for materialized KTable.
	 */
	private boolean loggingDisabled;

	/**
	 * Per input binding deserialization handler.
	 */
	private DeserializationExceptionHandler deserializationExceptionHandler;

	/**
	 * {@link org.apache.kafka.streams.processor.TimestampExtractor} bean name to use for this consumer.
	 */
	private String timestampExtractorBeanName;

	/**
	 * Comma separated list of supported event types for this binding.
	 */
	private String eventTypes;

	/**
	 * Record level header key for event type.
	 * If the default value is overridden, then that is expected on each record header if eventType based
	 * routing is enabled on this binding (by setting eventTypes).
	 */
	private String eventTypeHeaderKey = "event_type";

	/**
	 * Custom name for the source component from which the processor is consuming from.
	 */
	private String consumedAs;

	/**
	 * When event type based routing is enabled, the binder uses the byte[] Serde by default.
	 * Use this property to override this default behavior by forcing the binder to use the configured or inferred Serde.
	 */
	private boolean useConfiguredSerdeWhenRoutingEvents;

	public String getApplicationId() {
		return this.applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public String getKeySerde() {
		return this.keySerde;
	}

	public void setKeySerde(String keySerde) {
		this.keySerde = keySerde;
	}

	public String getValueSerde() {
		return this.valueSerde;
	}

	public void setValueSerde(String valueSerde) {
		this.valueSerde = valueSerde;
	}

	public String getMaterializedAs() {
		return this.materializedAs;
	}

	public void setMaterializedAs(String materializedAs) {
		this.materializedAs = materializedAs;
	}

	public boolean isCachingDisabled() {
		return this.cachingDisabled;
	}

	public void setCachingDisabled(boolean cachingDisabled) {
		this.cachingDisabled = cachingDisabled;
	}

	public boolean isLoggingDisabled() {
		return this.loggingDisabled;
	}

	public void setLoggingDisabled(boolean loggingDisabled) {
		this.loggingDisabled = loggingDisabled;
	}

	public String getTimestampExtractorBeanName() {
		return timestampExtractorBeanName;
	}

	public void setTimestampExtractorBeanName(String timestampExtractorBeanName) {
		this.timestampExtractorBeanName = timestampExtractorBeanName;
	}

	public DeserializationExceptionHandler getDeserializationExceptionHandler() {
		return deserializationExceptionHandler;
	}

	public void setDeserializationExceptionHandler(DeserializationExceptionHandler deserializationExceptionHandler) {
		this.deserializationExceptionHandler = deserializationExceptionHandler;
	}

	public String getEventTypes() {
		return eventTypes;
	}

	public void setEventTypes(String eventTypes) {
		this.eventTypes = eventTypes;
	}

	public String getEventTypeHeaderKey() {
		return this.eventTypeHeaderKey;
	}

	public void setEventTypeHeaderKey(String eventTypeHeaderKey) {
		this.eventTypeHeaderKey = eventTypeHeaderKey;
	}

	public String getConsumedAs() {
		return consumedAs;
	}

	public void setConsumedAs(String consumedAs) {
		this.consumedAs = consumedAs;
	}

	public boolean isUseConfiguredSerdeWhenRoutingEvents() {
		return this.useConfiguredSerdeWhenRoutingEvents;
	}

	public void setUseConfiguredSerdeWhenRoutingEvents(boolean useConfiguredSerdeWhenRoutingEvents) {
		this.useConfiguredSerdeWhenRoutingEvents = useConfiguredSerdeWhenRoutingEvents;
	}
}
