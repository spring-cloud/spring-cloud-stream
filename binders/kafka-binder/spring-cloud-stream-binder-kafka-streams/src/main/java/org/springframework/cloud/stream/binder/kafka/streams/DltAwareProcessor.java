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

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * @author Soby Chacko
 * @since 4.1.0
 */
public class DltAwareProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

	private final BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction;

	private final Supplier<Long> recordTimeSupplier;

	private String dltDestination;

	private DltSenderContext dltSenderContext;

	private BiConsumer<Record<KIn, VIn>, Exception> processorRecordRecoverer;

	private ProcessorContext<KOut, VOut> context;

	public DltAwareProcessor(BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction, String dltDestination,
							DltSenderContext dltSenderContext) {
		this(delegateFunction, dltDestination, dltSenderContext, System::currentTimeMillis);
	}

	public DltAwareProcessor(BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction, String dltDestination,
							DltSenderContext dltSenderContext, Supplier<Long> recordTimeSupplier) {
		this.delegateFunction = delegateFunction;
		this.recordTimeSupplier = recordTimeSupplier;
		Assert.isTrue(StringUtils.hasText(dltDestination), "DLT Destination topic must be provided.");
		this.dltDestination = dltDestination;
		Assert.notNull(dltSenderContext, "DltSenderContext cannot be null");
		this.dltSenderContext = dltSenderContext;
	}

	public DltAwareProcessor(BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction,
							BiConsumer<Record<KIn, VIn>, Exception> processorRecordRecoverer) {
		this(delegateFunction, processorRecordRecoverer, System::currentTimeMillis);
	}

	public DltAwareProcessor(BiFunction<KIn, VIn, KeyValue<KOut, VOut>> delegateFunction,
							BiConsumer<Record<KIn, VIn>, Exception> processorRecordRecoverer, Supplier<Long> recordTimeSupplier) {
		this.delegateFunction = delegateFunction;
		this.recordTimeSupplier = recordTimeSupplier;
		Assert.notNull(processorRecordRecoverer, "You must provide a valid processor recoverer");
		this.processorRecordRecoverer = processorRecordRecoverer;
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
			Record<KOut, VOut> downstreamRecord = new Record<>(keyValue.key, keyValue.value, recordTimeSupplier.get(), record.headers());
			this.context.forward(downstreamRecord);
		}
		catch (Exception exception) {
			if (this.processorRecordRecoverer == null) {
				this.processorRecordRecoverer = defaultProcessorRecordRecoverer();
			}
			this.processorRecordRecoverer.accept(record, exception);
		}
	}

	@Override
	public void close() {
		Processor.super.close();
	}

	BiConsumer<Record<KIn, VIn>, Exception> defaultProcessorRecordRecoverer() {
		return (r, e) -> {
			StreamBridge streamBridge = this.dltSenderContext.getStreamBridge();
			if (streamBridge != null) {
				streamBridge.send(this.dltDestination, r.value());
			}
		};
	}

}
