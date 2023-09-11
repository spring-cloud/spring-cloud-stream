/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar.properties;

import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;

/**
 * Pulsar producer properties used by the binder.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarProducerProperties extends ProducerConfigProperties {

	/**
	 * Pulsar {@link SchemaType} for this binding.
	 */
	@Nullable
	private SchemaType schemaType;

	/**
	 * Pulsar message type for this binding.
	 */
	@Nullable
	private Class<?> messageType;

	/**
	 * Pulsar message key type for this binding (only used when schema type is
	 * {@code }KEY_VALUE}).
	 */
	@Nullable
	private Class<?> messageKeyType;

	/**
	 * Number of topic partitions.
	 */
	@Nullable
	private Integer partitionCount;

	@Nullable
	public SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(@Nullable SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	@Nullable
	public Class<?> getMessageType() {
		return this.messageType;
	}

	public void setMessageType(@Nullable Class<?> messageType) {
		this.messageType = messageType;
	}

	@Nullable
	public Class<?> getMessageKeyType() {
		return this.messageKeyType;
	}

	public void setMessageKeyType(@Nullable Class<?> messageKeyType) {
		this.messageKeyType = messageKeyType;
	}

	@Nullable
	public Integer getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(Integer partitionCount) {
		this.partitionCount = partitionCount;
	}

}
