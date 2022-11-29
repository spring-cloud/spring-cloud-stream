/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;

import static org.assertj.core.api.Assertions.assertThat;

class BatchCapableRejectAndDontRequeueRecovererTest {

	@Test
	void testBatchRecoverThrowExceptionCorrectly() {
		MessageBatchRecoverer messageBatchRecoverer = new BatchCapableRejectAndDontRequeueRecoverer();

		Message expectedMessage1 = new Message(new byte[] {});
		Message expectedMessage2 = new Message(new byte[] {});

		List<Message> messages = List.of(expectedMessage1, expectedMessage2);

		Throwable expectedThrowable = new RuntimeException("test");

		ListenerExecutionFailedException exception = Assertions.assertThrows(ListenerExecutionFailedException.class,
				() -> messageBatchRecoverer.recover(messages, expectedThrowable));

		assertThat(exception.getFailedMessages()).contains(expectedMessage1);
		assertThat(exception.getFailedMessages()).contains(expectedMessage2);
		assertThat(exception.getCause()).isInstanceOf(AmqpRejectAndDontRequeueException.class);
		assertThat(expectedThrowable).isEqualTo(exception.getCause().getCause());
	}

	@Test
	void testRecoverThrowExceptionCorrectly() {
		MessageBatchRecoverer messageBatchRecoverer = new BatchCapableRejectAndDontRequeueRecoverer();

		Message expectedMessage = new Message(new byte[] {});

		Throwable expectedThrowable = new RuntimeException("test");

		ListenerExecutionFailedException exception = Assertions.assertThrows(ListenerExecutionFailedException.class,
				() -> messageBatchRecoverer.recover(expectedMessage, expectedThrowable));

		assertThat(expectedMessage).isEqualTo(exception.getFailedMessage());
		assertThat(exception.getCause()).isInstanceOf(AmqpRejectAndDontRequeueException.class);
		assertThat(expectedThrowable).isEqualTo(exception.getCause().getCause());
	}
}
