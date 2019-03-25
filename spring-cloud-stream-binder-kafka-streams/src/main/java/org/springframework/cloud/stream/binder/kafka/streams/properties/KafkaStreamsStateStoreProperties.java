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

/**
 * Properties for Kafka Streams state store.
 *
 * @author Lei Chen
 */
public class KafkaStreamsStateStoreProperties {

	/**
	 * Enumeration for store type.
	 */
	public enum StoreType {

		/**
		 * Key value store.
		 */
		KEYVALUE("keyvalue"),
		/**
		 * Window store.
		 */
		WINDOW("window"),
		/**
		 * Session store.
		 */
		SESSION("session");

		private final String type;

		StoreType(final String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}

	}

	/**
	 * Name for this state store.
	 */
	private String name;

	/**
	 * Type for this state store.
	 */
	private StoreType type;

	/**
	 * Size/length of this state store in ms. Only applicable for window store.
	 */
	private long length;

	/**
	 * Retention period for this state store in ms.
	 */
	private long retention;

	/**
	 * Key serde class specified per state store.
	 */
	private String keySerdeString;

	/**
	 * Value serde class specified per state store.
	 */
	private String valueSerdeString;

	/**
	 * Whether caching is enabled on this state store.
	 */
	private boolean cacheEnabled;

	/**
	 * Whether logging is enabled on this state store.
	 */
	private boolean loggingDisabled;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public StoreType getType() {
		return this.type;
	}

	public void setType(StoreType type) {
		this.type = type;
	}

	public long getLength() {
		return this.length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public long getRetention() {
		return this.retention;
	}

	public void setRetention(long retention) {
		this.retention = retention;
	}

	public String getKeySerdeString() {
		return this.keySerdeString;
	}

	public void setKeySerdeString(String keySerdeString) {
		this.keySerdeString = keySerdeString;
	}

	public String getValueSerdeString() {
		return this.valueSerdeString;
	}

	public void setValueSerdeString(String valueSerdeString) {
		this.valueSerdeString = valueSerdeString;
	}

	public boolean isCacheEnabled() {
		return this.cacheEnabled;
	}

	public void setCacheEnabled(boolean cacheEnabled) {
		this.cacheEnabled = cacheEnabled;
	}

	public boolean isLoggingDisabled() {
		return this.loggingDisabled;
	}

	public void setLoggingDisabled(boolean loggingDisabled) {
		this.loggingDisabled = loggingDisabled;
	}

}
