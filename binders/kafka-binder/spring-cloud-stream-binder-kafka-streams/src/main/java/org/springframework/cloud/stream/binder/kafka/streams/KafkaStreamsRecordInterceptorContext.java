/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Optional;

import org.apache.kafka.streams.processor.api.RecordMetadata;

import org.springframework.util.Assert;

/**
 * Context for a record intercepted by a {@link KafkaStreamsRecordInterceptor}.
 *
 * @author Nikita Kibitkin
 * @since 5.0.2
 */
public record KafkaStreamsRecordInterceptorContext(String bindingName, Optional<RecordMetadata> recordMetadata) {

	/**
	 * Create a new {@link KafkaStreamsRecordInterceptorContext}.
	 * @param bindingName the inbound binding name
	 * @param recordMetadata metadata for the intercepted record
	 */
	public KafkaStreamsRecordInterceptorContext {
		Assert.hasText(bindingName, "'bindingName' must not be empty");
		Assert.notNull(recordMetadata, "'recordMetadata' must not be null");
	}

	/**
	 * Return the source topic for the intercepted record, if available.
	 * @return the source topic
	 */
	public Optional<String> topic() {
		return this.recordMetadata.map(RecordMetadata::topic);
	}

	/**
	 * Return the source partition for the intercepted record, if available.
	 * @return the source partition
	 */
	public Optional<Integer> partition() {
		return this.recordMetadata.map(RecordMetadata::partition);
	}

	/**
	 * Return the source offset for the intercepted record, if available.
	 * @return the source offset
	 */
	public Optional<Long> offset() {
		return this.recordMetadata.map(RecordMetadata::offset);
	}

}
