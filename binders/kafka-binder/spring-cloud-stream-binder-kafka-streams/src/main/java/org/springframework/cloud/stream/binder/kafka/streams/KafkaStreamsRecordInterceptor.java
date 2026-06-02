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

import org.apache.kafka.streams.processor.api.Record;

/**
 * Interceptor invoked for records consumed by the Kafka Streams binder before the
 * user function sees the inbound {@code KStream}.
 *
 * @author Nikita Kibitkin
 * @since 5.0.2
 */
@FunctionalInterface
public interface KafkaStreamsRecordInterceptor {

	/**
	 * Intercept the inbound Kafka Streams record.
	 * @param record the inbound record
	 * @param context metadata about the intercepted record and binding
	 */
	void intercept(Record<?, ?> record, KafkaStreamsRecordInterceptorContext context);

}
