/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionHandlerTests {

	@Test
	void defaultPartitionSelectorDistributesUuidKeysAcrossPartitions() {
		int partitionCount = 16;
		int numberOfKeys = 10_000;
		int[] distribution = new int[partitionCount];

		PartitionHandler partitionHandler = createPartitionHandler(partitionCount);

		for (int i = 0; i < numberOfKeys; i++) {
			String key = new UUID(0L, i).toString();
			Message<String> message = MessageBuilder.withPayload(key).build();

			int partition = partitionHandler.determinePartition(message);
			distribution[partition]++;
		}

		int minimum = numberOfKeys / partitionCount;
		int maximum = 0;
		int actualMinimum = numberOfKeys;

		for (int count : distribution) {
			assertThat(count)
				.as("Each partition should receive a reasonable share of keys")
				.isGreaterThan(0);
			maximum = Math.max(maximum, count);
			actualMinimum = Math.min(actualMinimum, count);
		}

		assertThat(maximum - actualMinimum)
			.as("Difference between most- and least-used partitions: %s",
				distribution)
			.isLessThan(minimum / 2);
	}

	private PartitionHandler createPartitionHandler(int partitionCount) {
		ProducerProperties properties = new ProducerProperties();
		properties.setPartitionCount(partitionCount);
		properties.setPartitionKeyExpression(
			new SpelExpressionParser().parseExpression("payload"));

		return new PartitionHandler(new StandardEvaluationContext(), properties,
			new DefaultListableBeanFactory());
	}

}
