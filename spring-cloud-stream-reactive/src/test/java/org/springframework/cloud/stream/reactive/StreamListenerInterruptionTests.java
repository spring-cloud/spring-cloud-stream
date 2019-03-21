/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test validating that a fix for
 * <a href="https://github.com/reactor/reactor-core/issues/159"/> is present.
 *
 * @author Marius Bogoevici
 */
public class StreamListenerInterruptionTests {

	@Test
	public void testSubscribersNotInterrupted() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestTimeWindows.class, "--server.port=0");
		Sink sink = context.getBean(Sink.class);
		TestTimeWindows testTimeWindows = context.getBean(TestTimeWindows.class);
		sink.input().send(MessageBuilder.withPayload("hello1").build());
		sink.input().send(MessageBuilder.withPayload("hello2").build());
		sink.input().send(MessageBuilder.withPayload("hello3").build());
		assertThat(testTimeWindows.latch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(testTimeWindows.interruptionState).isNotNull();
		assertThat(testTimeWindows.interruptionState).isFalse();
		context.close();
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestTimeWindows {

		public CountDownLatch latch = new CountDownLatch(1);

		public Boolean interruptionState;

		@StreamListener
		public void receive(@Input(Sink.INPUT) Flux<String> input) {
			input.window(Duration.ofMillis(500), Duration.ofMillis(100))
					.flatMap(w -> w.reduce("", (x, y) -> x + y)).subscribe(x -> {
						this.interruptionState = Thread.currentThread().isInterrupted();
						this.latch.countDown();
					});
		}

	}

}
