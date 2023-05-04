/*
 * Copyright 2016-2020 the original author or authors.
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


package org.springframework.cloud.stream.binder.kafka;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.expression.EvaluationContext;
import org.springframework.messaging.Message;


/**
 * Extended default PartitionHandler with new method to determine partition with a kafka provided partition count.
 * @Author Oliver Führer
 */
public class KafkaPartitionHandler extends PartitionHandler {
	/**
	 * Construct a {@code KafkaPartitionHandler}.
	 * @param evaluationContext evaluation context for binder
	 * @param properties binder properties
	 * @param beanFactory instance of ConfigurableListableBeanFactory
	 *
	 * @since 4.0.3
	 */
	public KafkaPartitionHandler(EvaluationContext evaluationContext,
			ProducerProperties properties, ConfigurableListableBeanFactory beanFactory) {
		super(evaluationContext, properties, beanFactory);
	}

	/**
	 * @param message the message.
	 * @param partitionCount current partition count
	 * @return the partition
	 */
	public int determinePartition(Message<?> message, int partitionCount) {
		super.setPartitionCount(partitionCount);
		return super.determinePartition(message);
	}
}
