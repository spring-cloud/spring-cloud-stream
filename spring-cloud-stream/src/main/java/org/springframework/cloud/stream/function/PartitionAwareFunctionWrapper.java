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

import java.lang.reflect.Field;
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
import org.springframework.util.ReflectionUtils;

/**
 * This class is effectively a wrapper which is aware of the stream related partition information
 * for outgoing messages. It has only one responsibility and that is to modify the result message
 * with 'scst_partition' header if necessary.
 */
class PartitionAwareFunctionWrapper implements Function<Object, Object>, Supplier<Object> {

	protected final Log logger = LogFactory.getLog(PartitionAwareFunctionWrapper.class);

	private final FunctionInvocationWrapper function;

	private final Field enhancerFiled;

	@SuppressWarnings("rawtypes")
	private final Function<Object, Message> outputMessageEnricher;

	PartitionAwareFunctionWrapper(FunctionInvocationWrapper function, ConfigurableApplicationContext context, ProducerProperties producerProperties) {
		this.function = function;
		this.enhancerFiled = ReflectionUtils.findField(FunctionInvocationWrapper.class, "enhancer");
		this.enhancerFiled.setAccessible(true);
		if (producerProperties != null && producerProperties.isPartitioned()) {
			StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(context.getBeanFactory());
			PartitionHandler partitionHandler = new PartitionHandler(evaluationContext, producerProperties, context.getBeanFactory());

			this.outputMessageEnricher = output -> {
				if (!(output instanceof Message)) {
					output = MessageBuilder.withPayload(output).build();
				}
				int partitionId = partitionHandler.determinePartition((Message<?>) output);
				return MessageBuilder
					.fromMessage((Message<?>) output)
					.setHeader(BinderHeaders.PARTITION_HEADER, partitionId).build();
			};
		}
		else {
			this.outputMessageEnricher = null;
		}
	}

	@Override
	public Object apply(Object input) {
		this.setEnhancerIfNecessary();
		return this.function.apply(input);
	}

	@Override
	public Object get() {
		this.setEnhancerIfNecessary();
		return this.function.get();
	}

	private void setEnhancerIfNecessary() {
		try {
			this.enhancerFiled.set(this.function, this.outputMessageEnricher);
		}
		catch (Exception e) {
			logger.warn("Failed to set the enhancer", e);
		}
	}
}
