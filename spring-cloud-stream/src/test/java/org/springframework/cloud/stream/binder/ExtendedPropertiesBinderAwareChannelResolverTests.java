/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.ChannelBindingService;
import org.springframework.cloud.stream.binding.DefaultBindableChannelFactory;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public class ExtendedPropertiesBinderAwareChannelResolverTests
		extends BinderAwareChannelResolverTests {

	private volatile ExtendedPropertiesBinder<MessageChannel, ExtendedConsumerProperties, ExtendedProducerProperties> binder;

	private volatile List<TestBinder.TestBinding> producerBindings;

	@Before
	@Override
	public void setupContext() throws Exception {
		this.producerBindings = new ArrayList<>();
		this.binder = new TestBinder();
		BinderFactory binderFactory = new BinderFactory<MessageChannel>() {

			@Override
			public ExtendedPropertiesBinder<MessageChannel, ExtendedConsumerProperties, ExtendedProducerProperties> getBinder(
					String configurationName) {
				return ExtendedPropertiesBinderAwareChannelResolverTests.this.binder;
			}
		};
		this.channelBindingServiceProperties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<String, BindingProperties>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setContentType("text/plain");
		bindings.put("foo", bindingProperties);
		this.channelBindingServiceProperties.setBindings(bindings);
		MessageConverterConfigurer messageConverterConfigurer = new MessageConverterConfigurer(
				this.channelBindingServiceProperties, new DefaultMessageBuilderFactory(),
				new CompositeMessageConverterFactory());
		messageConverterConfigurer
				.setBeanFactory(Mockito.mock(ConfigurableListableBeanFactory.class));
		messageConverterConfigurer.afterPropertiesSet();
		this.bindableChannelFactory = new DefaultBindableChannelFactory(
				messageConverterConfigurer);
		this.dynamicDestinationsBindable = new DynamicDestinationsBindable();
		ChannelBindingService channelBindingService = new ChannelBindingService(
				this.channelBindingServiceProperties, binderFactory);
		this.resolver = new BinderAwareChannelResolver(channelBindingService,
				this.bindableChannelFactory, this.dynamicDestinationsBindable);
		this.resolver.setBeanFactory(this.context.getBeanFactory());
		this.context.getBeanFactory().registerSingleton("channelResolver", this.resolver);
		this.context.getBeanFactory().registerSingleton("dynamicDestinationBindable",
				this.dynamicDestinationsBindable);
		this.context.registerSingleton("other", DirectChannel.class);
		this.context.registerSingleton(
				IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				DefaultMessageBuilderFactory.class);
		this.context.getBeanFactory().registerSingleton("channelBindingService",
				channelBindingService);
		this.context.registerSingleton("inputBindingLifecycle",
				InputBindingLifecycle.class);
		this.context.registerSingleton("outputBindingLifecycle",
				OutputBindingLifecycle.class);
		this.context.refresh();
	}

	@Test
	@Override
	public void resolveChannel() {
		assertThat(this.producerBindings).hasSize(0);
		MessageChannel registered = this.resolver.resolveDestination("foo");
		assertThat(this.producerBindings).hasSize(1);
		TestBinder.TestBinding binding = this.producerBindings.get(0);
		assertThat(binding.isBound()).describedAs("Must be bound");
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
				new ExtendedConsumerProperties(new ConsumerProperties()));
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
		assertThat(received.get(0).getPayload()).isEqualTo("hello");
		this.context.close();
		assertThat(this.producerBindings).hasSize(1);
		assertThat(binding.isBound()).isFalse().describedAs("Must not be bound");
	}

	/**
	 * A simple test binder that creates queues for the destinations. Ignores groups.
	 */
	class TestBinder implements
			ExtendedPropertiesBinder<MessageChannel, ExtendedConsumerProperties, ExtendedProducerProperties> {

		private final Map<String, DirectChannel> destinations = new ConcurrentHashMap<>();

		@Override
		public Binding<MessageChannel> bindConsumer(String name, String group,
				MessageChannel inboundBindTarget, ExtendedConsumerProperties properties) {
			synchronized (this.destinations) {
				if (!this.destinations.containsKey(name)) {
					this.destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(inboundBindTarget);
			this.destinations.get(name).subscribe(directHandler);
			return new TestBinding(name, directHandler);
		}

		@Override
		public Binding<MessageChannel> bindProducer(String name,
				MessageChannel outboundBindTarget,
				ExtendedProducerProperties properties) {
			synchronized (this.destinations) {
				if (!this.destinations.containsKey(name)) {
					this.destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(this.destinations.get(name));
			// for test purposes we can assume it is a SubscribableChannel
			((SubscribableChannel) outboundBindTarget).subscribe(directHandler);
			TestBinding binding = new TestBinding(name, directHandler);
			ExtendedPropertiesBinderAwareChannelResolverTests.this.producerBindings
					.add(binding);
			return binding;
		}

		@Override
		public ExtendedConsumerProperties getExtendedConsumerProperties(
				String channelName) {
			return new ExtendedConsumerProperties(new ConsumerProperties());
		}

		@Override
		public ExtendedProducerProperties getExtendedProducerProperties(
				String channelName) {
			return new ExtendedProducerProperties(new ProducerProperties());
		}

		private final class TestBinding implements Binding<MessageChannel> {

			private final String name;

			private final DirectHandler directHandler;

			private boolean bound = true;

			private TestBinding(String name, DirectHandler directHandler) {
				this.name = name;
				this.directHandler = directHandler;
			}

			@Override
			public void unbind() {
				this.bound = false;
				TestBinder.this.destinations.get(this.name)
						.unsubscribe(this.directHandler);
			}

			public boolean isBound() {
				return this.bound;
			}
		}
	}
}
