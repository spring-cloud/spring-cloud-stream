/*
 * Copyright 2021-2021 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 *
 * {@link DeserializationExceptionHandler} that allows to silently skip
 * deserialization exceptions and continue processing.
 *
 * @author Soby Chakco
 * @since 3.1.2
 */
public class SkipAndContinueExceptionHandler implements DeserializationExceptionHandler {

	@Override
	public DeserializationExceptionHandler.DeserializationHandlerResponse handle(final ProcessorContext context,
																				final ConsumerRecord<byte[], byte[]> record,
																				final Exception exception) {
		return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
	}

	@Override
	public void configure(final Map<String, ?> configs) {
		// ignore
	}
}
