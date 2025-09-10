/*
 * Copyright 2023-present the original author or authors.
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

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Custom {@link RecordRecoverableProcessor} that is capable of sending the failed record to a DLT during recovery.
 *
 * @author Soby Chacko
 * @author Steven Gantz
 * @sinc 4.1.0
 */
public class DltAwareProcessor<KIn, VIn, KOut, VOut> extends RecordRecoverableProcessor<KIn, VIn, KOut, VOut> {

	private static final Log LOG = LogFactory.getLog(DltAwareProcessor.class);

	/**
	 * DLT destination.
	 */
	private final String dltDestination;

	/**
	 * {@link DltPublishingContext} used for DLT publishing needs.
	 */
	private final DltPublishingContext dltPublishingContext;

	/**
	 * A {@link BiConsumer} that does the recovery of a failed record.
	 */
	private BiConsumer<Record<KIn, VIn>, Exception> processorRecordRecoverer;

	/**
	 *
	 * @param delegateFunction {@link Function} to process the data
	 * @param dltDestination DLT destination
	 * @param dltPublishingContext {@link DltPublishingContext}
	 */
	public DltAwareProcessor(Function<Record<KIn, VIn>, Record<KOut, VOut>> delegateFunction, String dltDestination,
							DltPublishingContext dltPublishingContext) {
		super(delegateFunction);
		Assert.isTrue(StringUtils.hasText(dltDestination), "DLT Destination topic must be provided.");
		this.dltDestination = dltDestination;
		Assert.notNull(dltPublishingContext, "DltSenderContext cannot be null");
		this.dltPublishingContext = dltPublishingContext;
	}


	@Override
	protected BiConsumer<Record<KIn, VIn>, Exception> defaultProcessorRecordRecoverer() {
		return (r, e) -> {
			StreamBridge streamBridge = this.dltPublishingContext.getStreamBridge();
			if (streamBridge != null) {
				Message<VIn> message = MessageBuilder.withPayload(r.value())
					.setHeader(KafkaHeaders.KEY, r.key()).build();
				DltAwareProcessor.LOG.trace("Recovered from Exception: ", e);
				streamBridge.send(this.dltDestination, message);
			}
		};
	}
}
