package org.springframework.cloud.stream.binder;

import org.junit.jupiter.api.Test;
import org.springframework.core.retry.RetryTemplate;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MaxAttemptsRetryTests {

	@Test
	void maxAttemptsShouldNotExceedConfiguredAttempts() {

		ConsumerProperties props = new ConsumerProperties();
		props.setMaxAttempts(2);

		TestBinder binder = new TestBinder();

		RetryTemplate retryTemplate = binder.buildRetryTemplate(props);

		AtomicInteger attempts = new AtomicInteger();

		try {
			retryTemplate.execute(() -> {
				attempts.incrementAndGet();
				throw new RuntimeException("fail");
			});
		} catch (Exception ignored) {
		}

		assertThat(attempts.get()).isEqualTo(2);
	}

	static class TestBinder extends AbstractBinder<Object, ConsumerProperties, ProducerProperties> {

		@Override
		protected Binding<Object> doBindConsumer(String name, String group, Object inboundBindTarget,
												 ConsumerProperties consumerProperties) {
			return null;
		}

		@Override
		protected Binding<Object> doBindProducer(String name, Object outboundBindTarget,
												 ProducerProperties producerProperties) {
			return null;
		}
	}
}
