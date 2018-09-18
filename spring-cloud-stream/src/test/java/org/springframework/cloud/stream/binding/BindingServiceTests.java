/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.cloud.stream.utils.FooExtendedConsumerProperties;
import org.springframework.cloud.stream.utils.FooExtendedProducerProperties;
import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.cloud.stream.utils.MockExtendedBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Gary Russell
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 * @author Soby Chacko
 */
public class BindingServiceTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testDefaultGroup() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				inputChannelName);
		assertThat(bindings).hasSize(1);
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding).isSameAs(mockBinding);
		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding).unbind();
		binderFactory.destroy();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMultipleConsumerBindings() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockBinderFactory();

		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();

		Binding<MessageChannel> mockBinding1 = Mockito.mock(Binding.class);
		Binding<MessageChannel> mockBinding2 = Mockito.mock(Binding.class);

		when(binder.bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding1);
		when(binder.bindConsumer(eq("bar"), isNull(), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding2);

		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				"input");
		assertThat(bindings).hasSize(2);

		Iterator<Binding<MessageChannel>> iterator = bindings.iterator();
		Binding<MessageChannel> binding1 = iterator.next();
		Binding<MessageChannel> binding2 = iterator.next();

		assertThat(binding1).isSameAs(mockBinding1);
		assertThat(binding2).isSameAs(mockBinding2);

		service.unbindConsumers("input");

		verify(binder).bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binder).bindConsumer(eq("bar"), isNull(), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding1).unbind();
		verify(binding2).unbind();

		binderFactory.destroy();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testConsumerBindingWhenMultiplexingIsEnabled() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");

		ConsumerProperties consumer = properties.getConsumerProperties("input");
		consumer.setMultiplex(true);
		props.setConsumer(consumer);

		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockBinderFactory();

		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();

		Binding<MessageChannel> mockBinding1 = Mockito.mock(Binding.class);

		when(binder.bindConsumer(eq("foo,bar"), isNull(), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding1);

		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				"input");
		assertThat(bindings).hasSize(1);

		Iterator<Binding<MessageChannel>> iterator = bindings.iterator();
		Binding<MessageChannel> binding1 = iterator.next();

		assertThat(binding1).isSameAs(mockBinding1);

		service.unbindConsumers("input");

		verify(binder).bindConsumer(eq("foo,bar"), isNull(), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding1).unbind();

		binderFactory.destroy();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExplicitGroup() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		props.setGroup("fooGroup");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer(eq("foo"), eq("fooGroup"), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				inputChannelName);
		assertThat(bindings).hasSize(1);
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding).isSameAs(mockBinding);

		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer(eq("foo"), eq(props.getGroup()), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding).unbind();
		binderFactory.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void checkDynamicBinding() {
		BindingServiceProperties properties = new BindingServiceProperties();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(new ProducerProperties());
		properties.setBindings(Collections.singletonMap("foo", bindingProperties));
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		final ExtendedPropertiesBinder binder = mock(ExtendedPropertiesBinder.class);
		Properties extendedProps = new Properties();
		when(binder.getExtendedProducerProperties(anyString())).thenReturn(extendedProps);
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		final AtomicReference<MessageChannel> dynamic = new AtomicReference<>();
		when(binder.bindProducer(matches("foo"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(mockBinding);
		BindingService bindingService = new BindingService(properties, binderFactory) {

			@Override
			protected <T> Binder<T, ?, ?> getBinder(String channelName, Class<T> bindableType) {
				return binder;
			}

		};
		SubscribableChannelBindingTargetFactory bindableSubscribableChannelFactory =
				new SubscribableChannelBindingTargetFactory(
						new MessageConverterConfigurer(properties, new CompositeMessageConverterFactory()));
		final AtomicBoolean callbackInvoked = new AtomicBoolean();
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(
				bindingService, bindableSubscribableChannelFactory,
				new DynamicDestinationsBindable(),
				(name, channel, props, extended) -> {
					callbackInvoked.set(true);
					assertThat(name).isEqualTo("foo");
					assertThat(channel).isNotNull();
					assertThat(props).isNotNull();
					assertThat(extended).isSameAs(extendedProps);
					props.setUseNativeEncoding(true);
					extendedProps.setProperty("bar", "baz");
				});
		ConfigurableListableBeanFactory beanFactory = mock(ConfigurableListableBeanFactory.class);
		when(beanFactory.getBean("foo", MessageChannel.class))
				.thenThrow(new NoSuchBeanDefinitionException(MessageChannel.class));
		when(beanFactory.getBean("bar", MessageChannel.class))
				.thenThrow(new NoSuchBeanDefinitionException(MessageChannel.class));
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				dynamic.set(invocation.getArgument(1));
				return null;
			}

		}).when(beanFactory).registerSingleton(eq("foo"), any(MessageChannel.class));
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return dynamic.get();
			}

		}).when(beanFactory).initializeBean(any(MessageChannel.class), eq("foo"));
		resolver.setBeanFactory(beanFactory);
		MessageChannel resolved = resolver.resolveDestination("foo");
		assertThat(resolved).isSameAs(dynamic.get());
		ArgumentCaptor<ProducerProperties> captor = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("foo"), eq(dynamic.get()), captor.capture());
		assertThat(captor.getValue().isUseNativeEncoding()).isTrue();
		assertThat(captor.getValue()).isInstanceOf(ExtendedProducerProperties.class);
		assertThat(((ExtendedProducerProperties) captor.getValue()).getExtension()).isSameAs(extendedProps);
		doReturn(dynamic.get()).when(beanFactory).getBean("foo", MessageChannel.class);
		properties.setDynamicDestinations(new String[] { "foo" });
		resolved = resolver.resolveDestination("foo");
		assertThat(resolved).isSameAs(dynamic.get());
		properties.setDynamicDestinations(new String[] { "test" });
		try {
			resolved = resolver.resolveDestination("bar");
			fail();
		}
		catch (DestinationResolutionException e) {
			assertThat(e).hasMessageContaining("Failed to find MessageChannel bean with name 'bar'");
		}
	}

	@Test
	public void testProducerPropertiesValidation() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ProducerProperties producerProperties = new ProducerProperties();
		producerProperties.setPartitionCount(0);
		props.setDestination("foo");
		props.setProducer(producerProperties);
		final String outputChannelName = "output";
		bindingProperties.put(outputChannelName, props);
		serviceProperties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		BindingService service = new BindingService(serviceProperties, binderFactory);
		MessageChannel outputChannel = new DirectChannel();
		try {
			service.bindProducer(outputChannel, outputChannelName);
			fail("Producer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Partition count should be greater than zero.");
		}
	}

	@Test
	public void testExtendedDefaultProducerProperties() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ProducerProperties producerProperties = new ProducerProperties();
		props.setDestination("dest");
		props.setProducer(producerProperties);
		final String outputChannelName = "output";
		bindingProperties.put(outputChannelName, props);
		serviceProperties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockExtendedBinderFactory();

		ConfigurableApplicationContext applicationContext = new GenericApplicationContext();
		ConfigurableEnvironment environment = new StandardEnvironment();
		Map<String, Object> propertiesToAdd = new HashMap<>();
		propertiesToAdd.put("spring.cloud.stream.foo.default.producer.extendedProperty", "someFancyExtension");
		environment.getPropertySources().addLast(new MapPropertySource("extPropertiesConfig", propertiesToAdd));
		applicationContext.setEnvironment(environment);

		BindingService service = new BindingService(serviceProperties, binderFactory, null);
		service.setApplicationContext(applicationContext);
		MessageChannel outputChannel = new DirectChannel();

		Binder<MessageChannel, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		FooExtendedProducerProperties fooExtendedProducerProperties =
				(FooExtendedProducerProperties)((ExtendedPropertiesBinder)binder).getExtendedProducerProperties("output");
		assertThat(fooExtendedProducerProperties.getExtendedProperty()).isNull();

		service.bindProducer(outputChannel, outputChannelName);

		assertThat(fooExtendedProducerProperties.getExtendedProperty()).isEqualTo("someFancyExtension");
	}

	@Test
	public void testExtendedDefaultConsumerProperties() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ConsumerProperties consumerProperties = new ConsumerProperties();
		props.setDestination("dest");
		props.setConsumer(consumerProperties);
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		serviceProperties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockExtendedBinderFactory();

		ConfigurableApplicationContext applicationContext = new GenericApplicationContext();
		ConfigurableEnvironment environment = new StandardEnvironment();
		Map<String, Object> propertiesToAdd = new HashMap<>();
		propertiesToAdd.put("spring.cloud.stream.foo.default.consumer.extendedProperty", "someFancyExtension");
		environment.getPropertySources().addLast(new MapPropertySource("extPropertiesConfig", propertiesToAdd));
		applicationContext.setEnvironment(environment);

		BindingService service = new BindingService(serviceProperties, binderFactory, null);
		service.setApplicationContext(applicationContext);
		MessageChannel inputChannel = new DirectChannel();

		Binder<MessageChannel, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		FooExtendedConsumerProperties fooExtendedConsumerProperties =
				(FooExtendedConsumerProperties)((ExtendedPropertiesBinder)binder).getExtendedConsumerProperties("input");
		assertThat(fooExtendedConsumerProperties.getExtendedProperty()).isNull();

		service.bindConsumer(inputChannel, inputChannelName);

		assertThat(fooExtendedConsumerProperties.getExtendedProperty()).isEqualTo("someFancyExtension");
	}

	@Test
	public void testConsumerPropertiesValidation() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ConsumerProperties consumerProperties = new ConsumerProperties();
		consumerProperties.setConcurrency(0);
		props.setDestination("foo");
		props.setConsumer(consumerProperties);
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		serviceProperties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		BindingService service = new BindingService(serviceProperties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		try {
			service.bindConsumer(inputChannel, inputChannelName);
			fail("Consumer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Concurrency should be greater than zero.");
		}
	}

	@Test
	public void testUnknownBinderOnBindingFailure() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.input.binder", "mock");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.bindings.output.binder", "mockError");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(properties);
		BindingService bindingService = new BindingService(bindingServiceProperties,
				createMockBinderFactory());
		bindingService.bindConsumer(new DirectChannel(), "input");
		try {
			bindingService.bindProducer(new DirectChannel(), "output");
			fail("Expected 'Unknown binder configuration'");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Unknown binder configuration: mockError");
		}
	}

	@Test
	public void testUnrecognizedBinderAllowedIfNotUsed() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.defaultBinder", "mock1");
		properties.put("spring.cloud.stream.binders.mock1.type", "mock");
		properties.put("spring.cloud.stream.binders.kafka1.type", "kafka");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(properties);
		BinderFactory binderFactory = new BindingServiceConfiguration()
				.binderFactory(createMockBinderTypeRegistry(), bindingServiceProperties);
		BindingService bindingService = new BindingService(bindingServiceProperties,
				binderFactory);
		bindingService.bindConsumer(new DirectChannel(), "input");
		bindingService.bindProducer(new DirectChannel(), "output");
	}

	@Test
	public void testUnrecognizedBinderDisallowedIfUsed() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.input.binder", "mock1");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.bindings.output.type", "kafka1");
		properties.put("spring.cloud.stream.binders.mock1.type", "mock");
		properties.put("spring.cloud.stream.binders.kafka1.type", "kafka");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(properties);
		BinderFactory binderFactory = new BindingServiceConfiguration()
				.binderFactory(createMockBinderTypeRegistry(), bindingServiceProperties);
		BindingService bindingService = new BindingService(bindingServiceProperties,
				binderFactory);
		bindingService.bindConsumer(new DirectChannel(), "input");
		try {
			bindingService.bindProducer(new DirectChannel(), "output");
			fail("Expected 'Unknown binder configuration'");
		}
		catch (IllegalArgumentException e) {
			assertThat(e).hasMessageContaining("Binder type kafka is not defined");
		}
	}

	@Test
	public void testResolveBindableType() {
		Class<?> bindableType = GenericsUtils.getParameterType(FooBinder.class, Binder.class, 0);
		assertThat(bindableType).isSameAs(SomeBindableType.class);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testLateBindingConsumer() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		properties.setBindingRetryInterval(1);
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.initialize();
		BindingService service = new BindingService(properties, binderFactory, scheduler);
		MessageChannel inputChannel = new DirectChannel();
		final Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		final CountDownLatch fail = new CountDownLatch(2);
		doAnswer(i -> {
			fail.countDown();
			if (fail.getCount() == 1) {
				throw new RuntimeException("fail");
			}
			return mockBinding;
		}).when(binder).bindConsumer(eq("foo"), isNull(), same(inputChannel), any(ConsumerProperties.class));
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel, inputChannelName);
		assertThat(fail.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(bindings).hasSize(1);
		Binding<MessageChannel> delegate = TestUtils.getPropertyValue(bindings.iterator().next(), "delegate",
				Binding.class);
		int n = 0;
		while (n++ < 300 && delegate == null) {
			Thread.sleep(400);
		}
		assertThat(delegate).isSameAs(mockBinding);
		service.unbindConsumers(inputChannelName);
		verify(binder, times(2)).bindConsumer(eq("foo"), isNull(), same(inputChannel),
				any(ConsumerProperties.class));
		verify(delegate).unbind();
		binderFactory.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testLateBindingProducer() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		properties.setBindingRetryInterval(1);
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String outputChannelName = "output";
		bindingProperties.put(outputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.initialize();
		BindingService service = new BindingService(properties, binderFactory, scheduler);
		MessageChannel outputChannel = new DirectChannel();
		final Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		final CountDownLatch fail = new CountDownLatch(2);
		doAnswer(i -> {
			fail.countDown();
			if (fail.getCount() == 1) {
				throw new RuntimeException("fail");
			}
			return mockBinding;
		}).when(binder).bindProducer(eq("foo"), same(outputChannel), any(ProducerProperties.class));
		Binding<MessageChannel> binding = service.bindProducer(outputChannel, outputChannelName);
		assertThat(fail.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(binding).isNotNull();
		Binding delegate = TestUtils.getPropertyValue(binding, "delegate", Binding.class);
		int n = 0;
		while (n++ < 300 && delegate == null) {
			Thread.sleep(100);
			delegate = TestUtils.getPropertyValue(binding, "delegate", Binding.class);
		}
		assertThat(delegate).isSameAs(mockBinding);
		service.unbindProducers(outputChannelName);
		verify(binder, times(2)).bindProducer(eq("foo"), same(outputChannel), any(ProducerProperties.class));
		verify(delegate).unbind();
		binderFactory.destroy();
		scheduler.destroy();
	}

	private DefaultBinderFactory createMockBinderFactory() {
		BinderTypeRegistry binderTypeRegistry = createMockBinderTypeRegistry();
		return new DefaultBinderFactory(
				Collections.singletonMap("mock", new BinderConfiguration("mock", new HashMap<>(), true, true)),
				binderTypeRegistry);
	}

	private DefaultBinderFactory createMockExtendedBinderFactory() {
		BinderTypeRegistry binderTypeRegistry = createMockExtendedBinderTypeRegistry();
		return new DefaultBinderFactory(
				Collections.singletonMap("mock", new BinderConfiguration("mock", new HashMap<>(), true, true)),
				binderTypeRegistry);
	}


	private DefaultBinderTypeRegistry createMockBinderTypeRegistry() {
		return new DefaultBinderTypeRegistry(Collections.singletonMap("mock",
				new BinderType("mock", new Class[] { MockBinderConfiguration.class })));
	}

	private DefaultBinderTypeRegistry createMockExtendedBinderTypeRegistry() {
		return new DefaultBinderTypeRegistry(Collections.singletonMap("mock",
				new BinderType("mock", new Class[] { MockExtendedBinderConfiguration.class })));
	}

	private BindingServiceProperties createBindingServiceProperties(HashMap<String, String> properties) {
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		org.springframework.boot.context.properties.bind.Binder propertiesBinder = new org.springframework.boot.context.properties.bind.Binder(new MapConfigurationPropertySource(properties));
		propertiesBinder.bind("spring.cloud.stream", org.springframework.boot.context.properties.bind.Bindable.ofInstance(bindingServiceProperties));
		return bindingServiceProperties;
	}

	public static class FooBinder
			implements Binder<SomeBindableType, ConsumerProperties, ProducerProperties> {
		@Override
		public Binding<SomeBindableType> bindConsumer(String name, String group,
				SomeBindableType inboundBindTarget,
				ConsumerProperties consumerProperties) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Binding<SomeBindableType> bindProducer(String name,
				SomeBindableType outboundBindTarget,
				ProducerProperties producerProperties) {
			throw new UnsupportedOperationException();
		}
	}

	public static class SomeBindableType {
	}

}
