/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder;

import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Oleg Zhurakousky
 *
 * @since 1.2.2
 */
public class AbstractMessageChannelBinderTests {

	private ApplicationContext context;

	@Before
	public void prepare() {
		this.context = new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration())
				.web(WebApplicationType.NONE)
				.run();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEndpointLifecycle() throws Exception {
		AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, ProvisioningProvider<ConsumerProperties, ProducerProperties>> binder =
				context.getBean(AbstractMessageChannelBinder.class);

		ConsumerProperties consumerProperties = new ConsumerProperties();
		consumerProperties.setMaxAttempts(1); // to force error infrastructure creation

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup", new DirectChannel(), consumerProperties);
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(consumerBinding);
		MessageProducer messageProducer = (MessageProducer) consumerBindingAccessor.getPropertyValue("lifecycle");
		assertTrue(((Lifecycle)messageProducer).isRunning());
		assertNotNull(messageProducer.getOutputChannel());

		SubscribableChannel errorChannel = (SubscribableChannel) consumerBindingAccessor.getPropertyValue("lifecycle.errorChannel");
		assertThat(errorChannel).isNotNull();
		Set<MessageHandler> handlers = TestUtils.getPropertyValue(errorChannel, "dispatcher.handlers", Set.class);
		assertThat(handlers.size()).isEqualTo(2);
		Iterator<MessageHandler> iterator = handlers.iterator();
		assertThat(iterator.next()).isInstanceOf(BridgeHandler.class);
		assertThat(iterator.next()).isInstanceOf(LastSubscriberMessageHandler.class);
		assertThat(context.containsBean("foo.fooGroup.errors")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.recoverer")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.handler")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.bridge")).isTrue();
		consumerBinding.unbind();
		assertThat(context.containsBean("foo.fooGroup.errors")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.recoverer")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.handler")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.bridge")).isFalse();

		assertFalse(((Lifecycle) messageProducer).isRunning());

		ProducerProperties producerProps = new ProducerProperties();
		producerProps.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar", new DirectChannel(), producerProps);
		assertThat(context.containsBean("bar.errors")).isTrue();
		assertThat(context.containsBean("bar.errors.bridge")).isTrue();
		producerBinding.unbind();
		assertThat(context.containsBean("bar.errors")).isFalse();
		assertThat(context.containsBean("bar.errors.bridge")).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEndpointBinderHasRecoverer() throws Exception {
		ConfigurableApplicationContext context =
				new SpringApplicationBuilder(TestChannelBinderConfiguration.getCompleteConfiguration()).web(WebApplicationType.NONE).run();

		AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, ProvisioningProvider<ConsumerProperties, ProducerProperties>> binder =
				context.getBean(AbstractMessageChannelBinder.class);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup", new DirectChannel(), new ConsumerProperties());
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(consumerBinding);
		SubscribableChannel errorChannel = (SubscribableChannel) consumerBindingAccessor.getPropertyValue("lifecycle.errorChannel");
		assertThat(errorChannel).isNull();
		errorChannel = (SubscribableChannel) consumerBindingAccessor.getPropertyValue("lifecycle.recoveryCallback.channel");
		assertThat(errorChannel).isNotNull();
		Set<MessageHandler> handlers = TestUtils.getPropertyValue(errorChannel, "dispatcher.handlers", Set.class);
		assertThat(handlers.size()).isEqualTo(2);
		Iterator<MessageHandler> iterator = handlers.iterator();
		assertThat(iterator.next()).isInstanceOf(BridgeHandler.class);
		assertThat(iterator.next()).isInstanceOf(LastSubscriberMessageHandler.class);
		assertThat(context.containsBean("foo.fooGroup.errors")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.recoverer")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.handler")).isTrue();
		assertThat(context.containsBean("foo.fooGroup.errors.bridge")).isTrue();
		consumerBinding.unbind();
		assertThat(context.containsBean("foo.fooGroup.errors")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.recoverer")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.handler")).isFalse();
		assertThat(context.containsBean("foo.fooGroup.errors.bridge")).isFalse();
	}
}
