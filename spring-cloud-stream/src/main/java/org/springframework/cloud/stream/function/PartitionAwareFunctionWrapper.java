/*
 * Copyright 2020-2020 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

/**
 * This class is effectively a wrapper which is aware of the stream related partition information
 * for outgoing messages. It has only one responsibility and that is to modify the result message
 * with 'scst_partition' header if necessary.
 */
class PartitionAwareFunctionWrapper implements Function<Object, Object>, Supplier<Object> {

	protected final Log logger = LogFactory.getLog(PartitionAwareFunctionWrapper.class);

	@SuppressWarnings("rawtypes")
	private final Function function;

	@SuppressWarnings("rawtypes")
	private final Function<Message, Message> outputMessageEnricher;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	PartitionAwareFunctionWrapper(Function function, ConfigurableApplicationContext context, ProducerProperties producerProperties) {
		this.function = function;
		if (producerProperties != null && producerProperties.isPartitioned()) {
			StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(context.getBeanFactory());
			PartitionHandler partitionHandler = new PartitionHandler(evaluationContext, producerProperties, context.getBeanFactory());

			this.outputMessageEnricher = outputMessage -> {
				int partitionId = partitionHandler.determinePartition(outputMessage);
				return MessageBuilder
					.fromMessage(outputMessage)
					.setHeader(BinderHeaders.PARTITION_HEADER, partitionId).build();
			};
		}
		else {
			this.outputMessageEnricher = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object apply(Object input) {
		if (this.function instanceof FunctionInvocationWrapper && this.outputMessageEnricher != null) {
			try {
				return ((FunctionInvocationWrapper) this.function).apply(input, this.outputMessageEnricher);
			}
			catch (NoSuchMethodError e) {
				logger.warn("Versions of spring-cloud-function older then 3.0.2.RELEASE do not support generation of partition information. "
						+ "Output message will not contain any partition header unless spring-cloud-function dependency is 3.0.2.RELEASE or higher.");
				return this.function.apply(input);
			}
		}
		else {
			return this.function.apply(input);
		}
	}

	@Override
	public Object get() {
		if (this.function instanceof FunctionInvocationWrapper) {
			if (this.outputMessageEnricher != null) {
				return ((FunctionInvocationWrapper) this.function).get(this.outputMessageEnricher);
			}
			else {
				return ((FunctionInvocationWrapper) this.function).get();
			}
		}
		else {
			throw new IllegalStateException("THis function is not a Supplier. Call to get() is not allowed");
		}
	}
}
