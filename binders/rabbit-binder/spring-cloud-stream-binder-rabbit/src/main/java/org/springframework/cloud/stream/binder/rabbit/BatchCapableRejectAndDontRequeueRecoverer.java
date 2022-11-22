package org.springframework.cloud.stream.binder.rabbit;

import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageBatchRecoverer;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.util.Assert;

public class BatchCapableRejectAndDontRequeueRecoverer implements MessageBatchRecoverer {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR protected

	private final Supplier<String> messageSupplier;

	public BatchCapableRejectAndDontRequeueRecoverer() {
		this(() -> "Retry Policy Exhausted");
	}

	/**
	 * Construct an instance with the provided exception message supplier.
	 *
	 * @param messageSupplier the message supplier.
	 */
	public BatchCapableRejectAndDontRequeueRecoverer(Supplier<String> messageSupplier) {
		Assert.notNull(messageSupplier, "'messageSupplier' cannot be null");
		this.messageSupplier = messageSupplier;
	}

	@Override
	public void recover(List<Message> messages, Throwable cause) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Retries exhausted for message " + messages, cause);
		}
		throw new ListenerExecutionFailedException(this.messageSupplier.get(),
				new AmqpRejectAndDontRequeueException(cause), messages.toArray(Message[]::new));
	}

	@Override
	public void recover(Message message, Throwable cause) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Retries exhausted for message " + message, cause);
		}
		throw new ListenerExecutionFailedException(this.messageSupplier.get(),
				new AmqpRejectAndDontRequeueException(cause), message);
	}
}
