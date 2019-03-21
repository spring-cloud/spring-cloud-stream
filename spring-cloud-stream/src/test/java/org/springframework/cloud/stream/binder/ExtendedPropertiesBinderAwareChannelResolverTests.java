/*
 * Copyright 2013-2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class ExtendedPropertiesBinderAwareChannelResolverTests
		extends BinderAwareChannelResolverTests {

	@Test
	@Override
	public void resolveChannel() {
		Map<String, Bindable> bindables = this.context.getBeansOfType(Bindable.class);
		assertThat(bindables).hasSize(1);
		for (Bindable bindable : bindables.values()) {
			assertThat(bindable.getInputs().size()).isEqualTo(0); // producer
			assertThat(bindable.getOutputs().size()).isEqualTo(0); // consumer
		}
		MessageChannel registered = this.resolver.resolveDestination("foo");
		bindables = this.context.getBeansOfType(Bindable.class);
		assertThat(bindables).hasSize(1);
		for (Bindable bindable : bindables.values()) {
			assertThat(bindable.getInputs().size()).isEqualTo(0); // producer
			assertThat(bindable.getOutputs().size()).isEqualTo(1); // consumer
		}
		DirectChannel testChannel = new DirectChannel();
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message<?>> received = new ArrayList<>();
		testChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				received.add(message);
				latch.countDown();
			}
		});
		this.binder.bindConsumer("foo", null, testChannel,
				new ExtendedConsumerProperties<ConsumerProperties>(
						new ConsumerProperties()));
		assertThat(received).hasSize(0);
		registered.send(MessageBuilder.withPayload("hello").build());
		try {
			assertThat(latch.await(1, TimeUnit.SECONDS)).describedAs("latch timed out");
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("interrupted while awaiting latch");
		}
		assertThat(received).hasSize(1);
		assertThat(new String((byte[]) received.get(0).getPayload())).isEqualTo("hello");
		this.context.close();
		for (Bindable bindable : bindables.values()) {
			assertThat(bindable.getInputs().size()).isEqualTo(0);
			assertThat(bindable.getOutputs().size()).isEqualTo(0); // Must not be bound"
		}
	}

}
