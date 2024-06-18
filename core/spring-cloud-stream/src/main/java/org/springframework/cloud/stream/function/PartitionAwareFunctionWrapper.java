/*
 * Copyright 2020-2024 the original author or authors.
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
import org.springframework.util.ObjectUtils;

/**
 * This class is effectively a wrapper which is aware of the stream related partition information
 * for outgoing messages. It has only one responsibility and that is to modify the result message
 * with 'scst_partition' header if necessary.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author Byungjun You
 */
class PartitionAwareFunctionWrapper implements Function<Object, Object>, Supplier<Object> {

	protected final Log logger = LogFactory.getLog(PartitionAwareFunctionWrapper.class);

	@SuppressWarnings("rawtypes")
	protected final Function function;

	private final Function<Object, Object> outputMessageEnricher;

	private boolean messageEnricherEnabled = true;

	PartitionAwareFunctionWrapper(Function<?, ?> function, ConfigurableApplicationContext context, ProducerProperties producerProperties) {
		this.function = function;

		if (producerProperties != null && producerProperties.isPartitioned()) {
			StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(context.getBeanFactory());
			PartitionHandler partitionHandler = new PartitionHandler(evaluationContext, producerProperties, context.getBeanFactory());

			this.outputMessageEnricher = output -> {
				if ((ObjectUtils.isArray(output) && !(output instanceof byte[])) || output instanceof Iterable) {
					return output;
				}
				else if (!(output instanceof Message)) {
					output = MessageBuilder.withPayload(output).build();
				}
				return toMessageWithPartitionHeader((Message<?>) output, partitionHandler);
			};
		}
		else {
			this.outputMessageEnricher = null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Message<?> toMessageWithPartitionHeader(Message message, PartitionHandler partitionHandler) {
		int partitionId = partitionHandler.determinePartition(message);
		return MessageBuilder
			.fromMessage(message)
			.setHeader(BinderHeaders.PARTITION_HEADER, partitionId).build();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object apply(Object input) {
		if (this.messageEnricherEnabled) {
			this.setEnhancerIfNecessary();
		}
		Object result = this.function.apply(input);
		boolean messageContainsPartitionHeader = false;
		if (result != null && Message.class.isAssignableFrom(result.getClass())) {
			if (((Message<?>) result).getHeaders().containsKey(BinderHeaders.PARTITION_HEADER)) {
				messageContainsPartitionHeader = true;
			}
		}
		if (!((FunctionInvocationWrapper) this.function).isInputTypePublisher() && !messageContainsPartitionHeader) {
			((FunctionInvocationWrapper) this.function).setEnhancer(null);
		}
		return result;
	}

	@Override
	public Object get() {
		if (this.function instanceof FunctionInvocationWrapper functionInvocationWrapper) {
			if (this.messageEnricherEnabled) {
				this.setEnhancerIfNecessary();
			}
			return functionInvocationWrapper.get();
		}
		throw new IllegalStateException("Call to get() is not allowed since this function is not a Supplier.");
	}

	private void setEnhancerIfNecessary() {
		if (this.function instanceof FunctionInvocationWrapper functionInvocationWrapper) {
			functionInvocationWrapper.setEnhancer(this.outputMessageEnricher);
		}
	}

	public void setMessageEnricherEnabled(boolean messageEnricherEnabled) {
		this.messageEnricherEnabled = messageEnricherEnabled;
	}
}
