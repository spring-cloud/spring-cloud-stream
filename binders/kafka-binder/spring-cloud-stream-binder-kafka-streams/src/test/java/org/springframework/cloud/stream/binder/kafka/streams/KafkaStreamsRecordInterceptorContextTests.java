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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Nikita Kibitkin
 */
class KafkaStreamsRecordInterceptorContextTests {

	@Test
	void recordMetadataAccessorsReturnValuesWhenMetadataIsAvailable() {
		KafkaStreamsRecordInterceptorContext context = new KafkaStreamsRecordInterceptorContext("process-in-0",
				Optional.of(recordMetadata("foo", 1, 42L)));

		assertThat(context.topic()).contains("foo");
		assertThat(context.partition()).contains(1);
		assertThat(context.offset()).contains(42L);
	}

	@Test
	void recordMetadataAccessorsReturnEmptyWhenMetadataIsUnavailable() {
		KafkaStreamsRecordInterceptorContext context = new KafkaStreamsRecordInterceptorContext("process-in-0",
				Optional.empty());

		assertThat(context.topic()).isEmpty();
		assertThat(context.partition()).isEmpty();
		assertThat(context.offset()).isEmpty();
	}

	private RecordMetadata recordMetadata(String topic, int partition, long offset) {
		return new RecordMetadata() {

			@Override
			public String topic() {
				return topic;
			}

			@Override
			public int partition() {
				return partition;
			}

			@Override
			public long offset() {
				return offset;
			}

		};
	}

}
