package org.springframework.cloud.stream.binder.rabbit;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchCapableRejectAndDontRequeueRecovererTest {

	@Test
	void testBatchRecoverThrowExceptionCorrectly() {
		MessageBatchRecoverer messageBatchRecoverer = new BatchCapableRejectAndDontRequeueRecoverer();

		Message expectedMessage1 = new Message(new byte[] {});
		Message expectedMessage2 = new Message(new byte[] {});

		List<Message> messages = List.of(expectedMessage1, expectedMessage2);

		Throwable expectedThrowable = new RuntimeException("test");

		ListenerExecutionFailedException exception = assertThrows(ListenerExecutionFailedException.class,
				() -> messageBatchRecoverer.recover(messages, expectedThrowable));

		assertTrue(exception.getFailedMessages().contains(expectedMessage1));
		assertTrue(exception.getFailedMessages().contains(expectedMessage2));
		assertTrue(exception.getCause() instanceof AmqpRejectAndDontRequeueException);
		assertEquals(expectedThrowable, exception.getCause().getCause());

	}

	@Test
	void testRecoverThrowExceptionCorrectly() {
		MessageBatchRecoverer messageBatchRecoverer = new BatchCapableRejectAndDontRequeueRecoverer();

		Message expectedMessage = new Message(new byte[] {});

		Throwable expectedThrowable = new RuntimeException("test");

		ListenerExecutionFailedException exception = assertThrows(ListenerExecutionFailedException.class,
				() -> messageBatchRecoverer.recover(expectedMessage, expectedThrowable));

		assertEquals(expectedMessage, exception.getFailedMessage());
		assertTrue(exception.getCause() instanceof AmqpRejectAndDontRequeueException);
		assertEquals(expectedThrowable, exception.getCause().getCause());

	}

}