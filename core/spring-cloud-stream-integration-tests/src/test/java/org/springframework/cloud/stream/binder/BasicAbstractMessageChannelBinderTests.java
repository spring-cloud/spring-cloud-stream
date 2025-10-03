/*
 * Copyright 2017-present the original author or authors.
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

import java.util.Iterator;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @since 1.2.2
 */
@Disabled("This test has issues that needs to be looked into")
class BasicAbstractMessageChannelBinderTests {

	private static ApplicationContext context;

	@BeforeAll
	public static void prepare() {
		context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration())
			.web(WebApplicationType.NONE).run();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void endpointLifecycle() throws Exception {
		// @checkstyle:off
		AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, ProvisioningProvider<ConsumerProperties, ProducerProperties>> binder = context
			.getBean(AbstractMessageChannelBinder.class);
		// @checkstyle:on

		ConsumerProperties consumerProperties = new ConsumerProperties();
		consumerProperties.setMaxAttempts(1); // to force error infrastructure creation

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup",
			new DirectChannel(), consumerProperties);
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(
			consumerBinding);
		MessageProducer messageProducer = (MessageProducer) consumerBindingAccessor
			.getPropertyValue("lifecycle");
		assertThat(((Lifecycle) messageProducer).isRunning()).isTrue();
		assertThat(messageProducer.getOutputChannel()).isNotNull();

		SubscribableChannel errorChannel = (SubscribableChannel) consumerBindingAccessor
			.getPropertyValue("lifecycle.errorChannel");
		assertThat(errorChannel).isNotNull();
		Set<MessageHandler> handlers = TestUtils.getPropertyValue(errorChannel,
			"dispatcher.handlers", Set.class);
		assertThat(handlers.size()).isEqualTo(2);
		Iterator<MessageHandler> iterator = handlers.iterator();
		assertThat(iterator.next()).isInstanceOf(BridgeHandler.class);
		assertThat(iterator.next()).isInstanceOf(LastSubscriberMessageHandler.class);
		assertThat(this.context.containsBean("foo.fooGroup.errors")).isTrue();
		assertThat(this.context.containsBean("foo.fooGroup.errors.recoverer")).isTrue();
		assertThat(this.context.containsBean("foo.fooGroup.errors.handler")).isTrue();
		assertThat(this.context.containsBean("foo.fooGroup.errors.bridge")).isTrue();
		consumerBinding.unbind();
		assertThat(this.context.containsBean("foo.fooGroup.errors")).isFalse();
		assertThat(this.context.containsBean("foo.fooGroup.errors.recoverer")).isFalse();
		assertThat(this.context.containsBean("foo.fooGroup.errors.handler")).isFalse();
		assertThat(this.context.containsBean("foo.fooGroup.errors.bridge")).isFalse();

		assertThat(((Lifecycle) messageProducer).isRunning()).isFalse();

		ProducerProperties producerProps = new ProducerProperties();
		producerProps.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar",
			new DirectChannel(), producerProps);
		assertThat(this.context.containsBean("bar.errors")).isTrue();
		assertThat(this.context.containsBean("bar.errors.bridge")).isTrue();
		producerBinding.unbind();
		assertThat(this.context.containsBean("bar.errors")).isFalse();
		assertThat(this.context.containsBean("bar.errors.bridge")).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void endpointBinderHasRecoverer() throws Exception {
		// @checkstyle:off
		ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration())
			.web(WebApplicationType.NONE).run();

		AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, ProvisioningProvider<ConsumerProperties, ProducerProperties>> binder = context
			.getBean(AbstractMessageChannelBinder.class);
		// @checkstyle:on

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup",
			new DirectChannel(), new ConsumerProperties());
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(
			consumerBinding);
		SubscribableChannel errorChannel = (SubscribableChannel) consumerBindingAccessor
			.getPropertyValue("lifecycle.errorChannel");
		assertThat(errorChannel).isNull();
		errorChannel = (SubscribableChannel) consumerBindingAccessor
			.getPropertyValue("lifecycle.recoveryCallback.channel");
		assertThat(errorChannel).isNotNull();
		Set<MessageHandler> handlers = TestUtils.getPropertyValue(errorChannel,
			"dispatcher.handlers", Set.class);
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
