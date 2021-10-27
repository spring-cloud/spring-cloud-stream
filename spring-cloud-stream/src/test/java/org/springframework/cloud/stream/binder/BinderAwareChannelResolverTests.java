/*
 * Copyright 2013-2019 the original author or authors.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.GlobalChannelInterceptorWrapper;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ImmutableMessageChannelInterceptor;
import org.springframework.messaging.support.InterceptableChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class BinderAwareChannelResolverTests {

	protected ConfigurableApplicationContext context;

	protected volatile BinderAwareChannelResolver resolver;

	protected volatile Binder<MessageChannel, ConsumerProperties, ProducerProperties> binder;

	protected volatile SubscribableChannelBindingTargetFactory bindingTargetFactory;

	protected volatile BindingServiceProperties bindingServiceProperties;

	protected volatile DynamicDestinationsBindable dynamicDestinationsBindable;

	@SuppressWarnings("unchecked")
	@Before
	public void setupContext() throws Exception {

		this.context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						BinderAwareChannelResolverTests.InterceptorConfiguration.class))
								.web(WebApplicationType.NONE).run();

		this.resolver = this.context.getBean(BinderAwareChannelResolver.class);
		this.binder = this.context.getBean(Binder.class);
		this.bindingServiceProperties = this.context
				.getBean(BindingServiceProperties.class);
		this.bindingTargetFactory = this.context
				.getBean(SubscribableChannelBindingTargetFactory.class);
	}

	@Test
	public void resolveChannel() {
		Map<String, Bindable> bindables = this.context.getBeansOfType(Bindable.class);
		assertThat(bindables).hasSize(1);
		for (Bindable bindable : bindables.values()) {
			assertThat(bindable.getInputs().size()).isEqualTo(0); // producer
			assertThat(bindable.getOutputs().size()).isEqualTo(0); // consumer
		}
		MessageChannel registered = this.resolver.resolveDestination("foo");
		assertThat(((InterceptableChannel) registered).getInterceptors().size())
				.isEqualTo(2);
		assertThat(((InterceptableChannel) registered).getInterceptors()
				.get(1) instanceof ImmutableMessageChannelInterceptor).isTrue();

		bindables = this.context.getBeansOfType(Bindable.class);
		assertThat(bindables).hasSize(1);
		for (Bindable bindable : bindables.values()) {
			assertThat(bindable.getInputs().size()).isEqualTo(0); // producer
			assertThat(bindable.getOutputs().size()).isEqualTo(1); // consumer
		}
		DirectChannel testChannel = new DirectChannel();
		testChannel.setComponentName("INPUT");
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message<?>> received = new ArrayList<>();
		testChannel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				received.add(message);
				latch.countDown();
			}
		});
		this.binder.bindConsumer("foo", null, testChannel, new ConsumerProperties());
		assertThat(received).hasSize(0);
		registered.send(MessageBuilder.withPayload("hello").build());
		try {
			assertThat(latch.await(1, TimeUnit.SECONDS)).describedAs("Latch timed out");
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

	@Test
	public void resolveNonRegisteredChannel() {
		MessageChannel other = this.resolver.resolveDestination("other");
		assertThat(this.context.getBean("other")).isSameAs(other);
		this.context.close();
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void propertyPassthrough() {
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties genericProperties = new BindingProperties();
		genericProperties.setContentType("text/plain");
		bindings.put("foo", genericProperties);
		this.bindingServiceProperties.setBindings(bindings);
		Binder binder = mock(Binder.class);
		Binder binder2 = mock(Binder.class);
		BinderFactory mockBinderFactory = Mockito.mock(BinderFactory.class);
		Binding<MessageChannel> fooBinding = Mockito.mock(Binding.class);
		Binding<MessageChannel> barBinding = Mockito.mock(Binding.class);
		when(binder.bindProducer(matches("foo"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(fooBinding);
		when(binder2.bindProducer(matches("bar"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(barBinding);
		when(mockBinderFactory.getBinder(null, DirectWithAttributesChannel.class))
				.thenReturn(binder);
		when(mockBinderFactory.getBinder("someTransport",
				DirectWithAttributesChannel.class)).thenReturn(binder2);
		BindingService bindingService = new BindingService(this.bindingServiceProperties,
				mockBinderFactory, new ObjectMapper());
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(
				bindingService, this.bindingTargetFactory,
				new DynamicDestinationsBindable());
		resolver.setBeanFactory(this.context.getBeanFactory());
		SubscribableChannel resolved = (SubscribableChannel) resolver
				.resolveDestination("foo");
		verify(binder).bindProducer(eq("foo"), any(MessageChannel.class),
				any(ProducerProperties.class));
		assertThat(resolved).isSameAs(this.context.getBean("foo"));
		this.context.close();
	}

	@Configuration
	public static class InterceptorConfiguration {

		@Bean
		public GlobalChannelInterceptorWrapper testInterceptor() {
			return new GlobalChannelInterceptorWrapper(
					new ImmutableMessageChannelInterceptor());
		}

	}

}
