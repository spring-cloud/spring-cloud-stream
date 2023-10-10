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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

/**
 * Custom implementation for {@link ConsumerRecordRecoverer} that keeps a collection of
 * recoverer objects per input topics. These topics might be per input binding or multiplexed
 * topics in a single binding.
 *
 * @author Soby Chacko
 * @since 2.0.0
 */
public class SendToDlqAndContinue implements ConsumerRecordRecoverer {

	/**
	 * DLQ dispatcher per topic in the application context. The key here is not the actual
	 * DLQ topic but the incoming topic that caused the error.
	 */
	private final Map<String, DeadLetterPublishingRecoverer> dlqDispatchers = new HashMap<>();

	/**
	 * For a given topic, send the key/value record to DLQ topic.
	 *
	 * @param consumerRecord consumer record
	 * @param exception exception
	 */
	public void sendToDlq(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
		DeadLetterPublishingRecoverer kafkaStreamsDlqDispatch = this.dlqDispatchers.get(consumerRecord.topic());
		kafkaStreamsDlqDispatch.accept(consumerRecord, exception);
	}

	void addKStreamDlqDispatch(String topic,
			DeadLetterPublishingRecoverer kafkaStreamsDlqDispatch) {
		this.dlqDispatchers.put(topic, kafkaStreamsDlqDispatch);
	}

	@Override
	public void accept(ConsumerRecord<?, ?> consumerRecord, Exception e) {
		this.dlqDispatchers.get(consumerRecord.topic()).accept(consumerRecord, e);
	}
}
