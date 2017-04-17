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

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder.ErrorInfrastructure;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @since 1.2.2
 */
public class AbstractMessageChannelBinderTests {

	@Test
	public void testEndpointLifecycle() throws Exception {
		StubMessageChannelBinder binder = new StubMessageChannelBinder();
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		context.getBeanFactory().registerSingleton("errorChannel", new PublishSubscribeChannel());
		binder.setApplicationContext(context);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup", new DirectChannel(),
				new ConsumerProperties());
		DirectFieldAccessor consumerBindingAccessor = new DirectFieldAccessor(consumerBinding);
		Object messageProducer = consumerBindingAccessor.getPropertyValue("lifecycle");
		Mockito.verify((Lifecycle) messageProducer).start();
		Mockito.verify((InitializingBean) messageProducer).afterPropertiesSet();
		Mockito.verify((MessageProducer) messageProducer).setOutputChannel(Mockito.any(MessageChannel.class));
		Mockito.verifyNoMoreInteractions(messageProducer);
		ErrorInfrastructure errorInfra = binder.errorInfrastructure;
		SubscribableChannel errorChannel = errorInfra.getErrorChannel();
		assertThat(errorChannel).isNotNull();
		@SuppressWarnings("unchecked")
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

		Mockito.verify((Lifecycle) messageProducer).stop();
		Mockito.verify((DisposableBean) messageProducer).destroy();
		Mockito.verifyNoMoreInteractions(messageProducer);

		Binding<MessageChannel> producerBinding = binder.bindProducer("bar", new DirectChannel(),
				new ProducerProperties());
		DirectFieldAccessor producerBindingAccessor = new DirectFieldAccessor(producerBinding);
		Object messageHandler = producerBindingAccessor.getPropertyValue("lifecycle");
		Mockito.verify((Lifecycle) messageHandler).start();
		Mockito.verify((InitializingBean) messageHandler).afterPropertiesSet();
		Mockito.verifyNoMoreInteractions(messageHandler);
		producerBinding.unbind();
		Mockito.verify((Lifecycle) messageHandler).stop();
		Mockito.verify((DisposableBean) messageHandler).destroy();
		Mockito.verifyNoMoreInteractions(messageHandler);
	}

	@Test
	public void testEndpointBinderHasRecoverer() throws Exception {
		StubMessageChannelBinder binder = new StubMessageChannelBinder(true);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		context.getBeanFactory().registerSingleton("errorChannel", new PublishSubscribeChannel());
		binder.setApplicationContext(context);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo", "fooGroup", new DirectChannel(),
				new ConsumerProperties());
		ErrorInfrastructure errorInfra = binder.errorInfrastructure;
		SubscribableChannel errorChannel = errorInfra.getErrorChannel();
		assertThat(errorChannel).isNotNull();
		@SuppressWarnings("unchecked")
		Set<MessageHandler> handlers = TestUtils.getPropertyValue(errorChannel, "dispatcher.handlers", Set.class);
		assertThat(handlers.size()).isEqualTo(2);
		Iterator<MessageHandler> iterator = handlers.iterator();
		assertThat(iterator.next()).isInstanceOf(BridgeHandler.class);
		assertThat(iterator.next()).isNotInstanceOf(LastSubscriberMessageHandler.class);
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

	private static class StubMessageChannelBinder extends
			AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties,
										ProvisioningProvider<ConsumerProperties, ProducerProperties>> {

		private final boolean hasRecoverer;

		private ErrorInfrastructure errorInfrastructure;

		StubMessageChannelBinder() {
			this(false);
		}

		@SuppressWarnings("unchecked")
		StubMessageChannelBinder(boolean hasRecoverer) {
			super(true, null, Mockito.mock(ProvisioningProvider.class));
			mockProvisioner();
			this.hasRecoverer = hasRecoverer;
		}

		private void mockProvisioner() {
			willAnswer(new Answer<SimpleConsumerDestination>() {

				@Override
				public SimpleConsumerDestination answer(final InvocationOnMock invocation) throws Throwable {
					return new SimpleConsumerDestination(invocation.getArgumentAt(0, String.class));
				}

			}).given(this.provisioningProvider).provisionConsumerDestination(anyString(), anyString(),
					Matchers.any(ConsumerProperties.class));
		}

		@Override
		protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
				ProducerProperties producerProperties) throws Exception {
			MessageHandler mock = Mockito.mock(MessageHandler.class, Mockito.withSettings()
					.extraInterfaces(Lifecycle.class, InitializingBean.class, DisposableBean.class));
			return mock;
		}

		@Override
		protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
				ConsumerProperties properties) throws Exception {
			this.errorInfrastructure = registerErrorInfrastructure(destination, group, properties);
			MessageProducer adapter = Mockito.mock(MessageProducer.class,
					Mockito.withSettings().extraInterfaces(Lifecycle.class, InitializingBean.class,
							DisposableBean.class));
			return adapter;
		}

		@Override
		protected MessageHandler getErrorMessageHandler(ConsumerDestination destination, String group,
				ConsumerProperties consumerProperties) {
			if (this.hasRecoverer) {
				return mock(MessageHandler.class);
			}
			else {
				return null;
			}
		}

	}

	private static class SimpleConsumerDestination implements ConsumerDestination {

		private final String name;

		SimpleConsumerDestination(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return this.name;
		}

	}

}
