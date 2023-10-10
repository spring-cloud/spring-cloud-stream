/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.DeserializationExceptionHandler;

/**
 * Kafka Streams binder configuration properties.
 *
 * @author Soby Chacko
 * @author Gary Russell
 */
public class KafkaStreamsBinderConfigurationProperties
		extends KafkaBinderConfigurationProperties {

	public KafkaStreamsBinderConfigurationProperties(KafkaProperties kafkaProperties) {
		super(kafkaProperties);
	}

	private String applicationId;

	private StateStoreRetry stateStoreRetry = new StateStoreRetry();

	private Map<String, Functions> functions = new HashMap<>();

	/**
	 * {@link org.apache.kafka.streams.errors.DeserializationExceptionHandler} to use when
	 * there is a deserialization exception. This handler will be applied against all input bindings
	 * unless overridden at the consumer binding.
	 */
	private DeserializationExceptionHandler deserializationExceptionHandler;

	private boolean includeStoppedProcessorsForHealthCheck;

	public Map<String, Functions> getFunctions() {
		return functions;
	}

	public void setFunctions(Map<String, Functions> functions) {
		this.functions = functions;
	}

	public StateStoreRetry getStateStoreRetry() {
		return stateStoreRetry;
	}

	public void setStateStoreRetry(StateStoreRetry stateStoreRetry) {
		this.stateStoreRetry = stateStoreRetry;
	}

	public String getApplicationId() {
		return this.applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public DeserializationExceptionHandler getDeserializationExceptionHandler() {
		return deserializationExceptionHandler;
	}

	public void setDeserializationExceptionHandler(DeserializationExceptionHandler deserializationExceptionHandler) {
		this.deserializationExceptionHandler = deserializationExceptionHandler;
	}

	public boolean isIncludeStoppedProcessorsForHealthCheck() {
		return includeStoppedProcessorsForHealthCheck;
	}

	public void setIncludeStoppedProcessorsForHealthCheck(boolean includeStoppedProcessorsForHealthCheck) {
		this.includeStoppedProcessorsForHealthCheck = includeStoppedProcessorsForHealthCheck;
	}

	public static class StateStoreRetry {

		private int maxAttempts = 1;

		private long backoffPeriod = 1000;

		public int getMaxAttempts() {
			return maxAttempts;
		}

		public void setMaxAttempts(int maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

		public long getBackoffPeriod() {
			return backoffPeriod;
		}

		public void setBackoffPeriod(long backoffPeriod) {
			this.backoffPeriod = backoffPeriod;
		}
	}

	public static class Functions {

		/**
		 * Function specific application id.
		 */
		private String applicationId;

		/**
		 * Funcion specific configuraiton to use.
		 */
		private Map<String, String> configuration;

		public String getApplicationId() {
			return applicationId;
		}

		public void setApplicationId(String applicationId) {
			this.applicationId = applicationId;
		}

		public Map<String, String> getConfiguration() {
			return configuration;
		}

		public void setConfiguration(Map<String, String> configuration) {
			this.configuration = configuration;
		}
	}

}
