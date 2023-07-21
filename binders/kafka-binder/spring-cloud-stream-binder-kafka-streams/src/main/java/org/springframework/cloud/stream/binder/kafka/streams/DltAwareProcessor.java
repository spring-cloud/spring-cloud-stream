/*
 * Copyright 2023-2023 the original author or authors.
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

import java.util.function.BiFunction;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.cloud.stream.function.StreamBridge;

/**
 *
 * @author Soby Chacko
 * @since 4.1.0
 */
public class DltAwareProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

	private final BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction;

	private ProcessorContext<KOut, VOut> context;

	private final String dltDestination;

	private final DltSenderContext dltSenderContext;

	public DltAwareProcessor(BiFunction<KIn, VIn, KeyValue<KOut, VOut>> businessLogic, String dltDestination, DltSenderContext dltSenderContext) {
		this.delegateFunction = businessLogic;
		this.dltDestination = dltDestination;
		this.dltSenderContext = dltSenderContext;
	}

	@Override
	public void init(ProcessorContext<KOut, VOut> context) {
		Processor.super.init(context);
		this.context = context;
	}

	@Override
	public void process(Record<KIn, VIn> record) {
		try {
			KeyValue<KOut, VOut> keyValue = this.delegateFunction.apply(record.key(), record.value());
			//TODO: What should be the timestamp?
			Record<KOut, VOut> downstreamRecord = new Record<>(keyValue.key, keyValue.value, System.currentTimeMillis(), record.headers());
			this.context.forward(downstreamRecord);
		}
		catch (Exception exception) {
			StreamBridge streamBridge = this.dltSenderContext.getStreamBridge();
			if (streamBridge != null) {
				streamBridge.send(dltDestination, record.value());
			}
		}
	}

	@Override
	public void close() {
		Processor.super.close();
	}

}
