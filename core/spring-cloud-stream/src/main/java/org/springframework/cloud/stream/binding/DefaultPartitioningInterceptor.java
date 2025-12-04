/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;

/**
 *
 * @author Oleg Zhurakousky
 * @since 3.1
 *
 */
public class DefaultPartitioningInterceptor implements ChannelInterceptor {

	private final PartitionHandler partitionHandler;

	public DefaultPartitioningInterceptor(BindingProperties bindingProperties, ConfigurableListableBeanFactory beanFactory) {
		this.partitionHandler = new PartitionHandler(
				ExpressionUtils.createStandardEvaluationContext(beanFactory),
				bindingProperties.getProducer(), beanFactory);
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionHandler.setPartitionCount(partitionCount);
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {
		if (!message.getHeaders().containsKey(BinderHeaders.PARTITION_OVERRIDE)) {
			int partition = this.partitionHandler.determinePartition(message);
			return MessageBuilder
					.fromMessage(message)
					.setHeader(BinderHeaders.PARTITION_HEADER, partition).build();
		}
		else {
			return MessageBuilder
					.fromMessage(message)
					.setHeader(BinderHeaders.PARTITION_HEADER,
							message.getHeaders()
									.get(BinderHeaders.PARTITION_OVERRIDE))
					.removeHeader(BinderHeaders.PARTITION_OVERRIDE).build();
		}
	}

}

