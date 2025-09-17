/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderChildContextInitializer;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.test.EnableTestBinder;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.cloud.stream.utils.IntegrationTestsMockBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
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
 * @author Michael Michailidis
 * @author Chris Bono
 * @author Artem Bilan
 * @author Kotaro Matsumoto
 * @author Omer Celik
 */
class BindingServiceTests {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void defaultGroup() {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = createMockBinderFactory();
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
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
	void multipleConsumerBindings() {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");
		String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockBinderFactory();

		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
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

		assertThat(service.getConsumerBindings("input")).containsSequence(bindings);

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
	void multipleConsumerBindingsFromIndexList() {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");

		ConsumerProperties consumer = properties.getConsumerProperties("input");
		consumer.setInstanceIndexList(Arrays.asList(0, 1));
		consumer.setInstanceCount(2);
		consumer.setPartitioned(true);
		props.setConsumer(consumer);

		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = createMockBinderFactory();

		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
		MessageChannel inputChannel = new DirectChannel();

		Binding<MessageChannel> mockBinding1 = Mockito.mock(Binding.class, "FirstBinding");
		Binding<MessageChannel> mockBinding2 = Mockito.mock(Binding.class, "SecondBinding");

		ArgumentCaptor<ConsumerProperties> captor = ArgumentCaptor.forClass(ConsumerProperties.class);

		when(binder.bindConsumer(eq("foo"), isNull(), same(inputChannel),
			any(ConsumerProperties.class))).thenReturn(mockBinding1).thenReturn(mockBinding2);

		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
			"input");
		assertThat(bindings).hasSize(2);

		Iterator<Binding<MessageChannel>> iterator = bindings.iterator();
		Binding<MessageChannel> binding1 = iterator.next();
		Binding<MessageChannel> binding2 = iterator.next();

		assertThat(binding1).isSameAs(mockBinding1);
		assertThat(binding2).isSameAs(mockBinding2);

		service.unbindConsumers("input");

		verify(binder, times(2)).bindConsumer(eq("foo"), isNull(), same(inputChannel),
			captor.capture());
		verify(binding1).unbind();
		verify(binding2).unbind();

		List<ConsumerProperties> allValues = captor.getAllValues();

		assertThat(allValues.size()).isEqualTo(2);

		assertThat(allValues.get(0).getInstanceIndex()).isEqualTo(0);
		assertThat(allValues.get(1).getInstanceIndex()).isEqualTo(1);

		binderFactory.destroy();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void consumerBindingWhenMultiplexingIsEnabled() {
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
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
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
	void explicitGroup() {
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
		BindingService service = new BindingService(properties, binderFactory, new ObjectMapper());
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

	@Test
	@Disabled
	void producerPropertiesValidation() {
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
		BindingService service = new BindingService(serviceProperties, binderFactory, new ObjectMapper());
		MessageChannel outputChannel = new DirectChannel();
		try {
			service.bindProducer(outputChannel, outputChannelName);
			fail("Producer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e)
				.hasMessageContaining("Partition count should be greater than zero.");
		}
	}

	@Test
	void defaultPropertyBehavior() {
		ConfigurableApplicationContext run = SpringApplication.run(
			DefaultConsumerPropertiesTestSink.class,
			"--server.port=0",
			"--spring.cloud.stream.output-bindings=output1",
			"--spring.cloud.stream.input-bindings=inputFooBarBuzz",
			"--spring.cloud.stream.default.contentType=text/plain",
			"--spring.cloud.stream.bindings.input1.contentType=application/json",
			"--spring.cloud.stream.default.group=foo",
			"--spring.cloud.stream.bindings.input2.group=bar",
			"--spring.cloud.stream.default.consumer.concurrency=5",
			"--spring.cloud.stream.bindings.input2.consumer.concurrency=1",
			"--spring.cloud.stream.bindings.input1.consumer.partitioned=true",
			"--spring.cloud.stream.default.producer.partitionCount=10",
			"--spring.cloud.stream.bindings.output2.producer.partitionCount=1",
			"--spring.cloud.stream.bindings.inputXyz.contentType=application/json",
			"--spring.cloud.stream.bindings.inputFooBar.contentType=application/avro",
			"--spring.cloud.stream.bindings.input_snake_case.contentType=application/avro");

		BindingServiceProperties bindingServiceProperties = run.getBeanFactory()
			.getBean(BindingServiceProperties.class);
		Map<String, BindingProperties> bindings = bindingServiceProperties.getBindings();

		assertThat(bindings.get("input1").getContentType()).isEqualTo("application/json");
		assertThat(bindings.get("input2").getContentType()).isEqualTo("text/plain");
		assertThat(bindings.get("input1").getGroup()).isEqualTo("foo");
		assertThat(bindings.get("input2").getGroup()).isEqualTo("bar");
		assertThat(bindings.get("input1").getConsumer().getConcurrency()).isEqualTo(5);
		assertThat(bindings.get("input2").getConsumer().getConcurrency()).isEqualTo(1);
		assertThat(bindings.get("input1").getConsumer().isPartitioned()).isEqualTo(true);
		assertThat(bindings.get("input2").getConsumer().isPartitioned()).isEqualTo(false);
		assertThat(bindings.get("output1").getProducer().getPartitionCount())
			.isEqualTo(10);
		assertThat(bindings.get("output2").getProducer().getPartitionCount())
			.isEqualTo(1);

		assertThat(bindings.get("inputXyz").getContentType())
			.isEqualTo("application/json");
		assertThat(bindings.get("inputFooBar").getContentType())
			.isEqualTo("application/avro");
		assertThat(bindings.get("inputFooBarBuzz").getContentType())
			.isEqualTo("text/plain");
		assertThat(bindings.get("input_snake_case").getContentType())
			.isEqualTo("application/avro");

		run.close();
	}

	@Test
	@Disabled
	void consumerPropertiesValidation() {
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
		BindingService service = new BindingService(serviceProperties, binderFactory, new ObjectMapper());
		MessageChannel inputChannel = new DirectChannel();
		try {
			service.bindConsumer(inputChannel, inputChannelName);
			fail("Consumer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e)
				.hasMessageContaining("Concurrency should be greater than zero.");
		}
	}

	@Test
	void unknownBinderOnBindingFailure() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.input.binder", "mock");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.bindings.output.binder", "mockError");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(
			properties);
		BindingService bindingService = new BindingService(bindingServiceProperties,
			createMockBinderFactory(), new ObjectMapper());
		bindingService.bindConsumer(new DirectChannel(), "input");
		try {
			bindingService.bindProducer(new DirectChannel(), "output");
			fail("Expected 'Unknown binder configuration'");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Unknown binder configuration: mockError");
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void unrecognizedBinderAllowedIfNotUsed() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.defaultBinder", "mock1");
		properties.put("spring.cloud.stream.binders.mock1.type", "mock");
		properties.put("spring.cloud.stream.binders.kafka1.type", "kafka");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(
			properties);
		BinderFactory binderFactory = new BindingServiceConfiguration()
			.binderFactory(createMockBinderTypeRegistry(), bindingServiceProperties, Mockito.mock(ObjectProvider.class),
				Mockito.mock(BinderChildContextInitializer.class));
		BindingService bindingService = new BindingService(bindingServiceProperties,
			binderFactory, new ObjectMapper());
		bindingService.bindConsumer(new DirectChannel(), "input");
		bindingService.bindProducer(new DirectChannel(), "output");
	}

	@SuppressWarnings("unchecked")
	@Test
	void unrecognizedBinderDisallowedIfUsed() {
		HashMap<String, String> properties = new HashMap<>();
		properties.put("spring.cloud.stream.bindings.input.destination", "fooInput");
		properties.put("spring.cloud.stream.bindings.input.binder", "mock1");
		properties.put("spring.cloud.stream.bindings.output.destination", "fooOutput");
		properties.put("spring.cloud.stream.bindings.output.type", "kafka1");
		properties.put("spring.cloud.stream.binders.mock1.type", "mock");
		properties.put("spring.cloud.stream.binders.kafka1.type", "kafka");
		BindingServiceProperties bindingServiceProperties = createBindingServiceProperties(
			properties);
		BinderFactory binderFactory = new BindingServiceConfiguration()
			.binderFactory(createMockBinderTypeRegistry(), bindingServiceProperties, Mockito.mock(ObjectProvider.class),
				Mockito.mock(BinderChildContextInitializer.class));
		BindingService bindingService = new BindingService(bindingServiceProperties,
			binderFactory, new ObjectMapper());
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
	void resolveBindableType() {
		Class<?> bindableType = GenericsUtils.getParameterType(FooBinder.class,
			Binder.class, 0);
		assertThat(bindableType).isSameAs(SomeBindableType.class);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void lateBindingProducer() throws Exception {
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
		BindingService service = new BindingService(properties, binderFactory, scheduler, new ObjectMapper());
		MessageChannel outputChannel = new DirectChannel();
		final Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		final CountDownLatch fail = new CountDownLatch(2);
		doAnswer(i -> {
			fail.countDown();
			if (fail.getCount() == 1) {
				throw new RuntimeException("fail");
			}
			return mockBinding;
		}).when(binder).bindProducer(eq("foo"), same(outputChannel),
			any(ProducerProperties.class));
		Binding<MessageChannel> binding = service.bindProducer(outputChannel,
			outputChannelName);
		assertThat(fail.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(binding).isNotNull();
		Binding delegate = TestUtils.getPropertyValue(binding, "delegate", Binding.class);
		int n = 0;
		while (n++ < 300 && delegate == null) {
			Thread.sleep(100);
			delegate = TestUtils.getPropertyValue(binding, "delegate", Binding.class);
		}
		assertThat(delegate).isSameAs(mockBinding);

		assertThat(service.getProducerBinding("output")).isSameAs(binding);

		service.unbindProducers(null, outputChannelName);
		verify(binder, times(2)).bindProducer(eq("foo"), same(outputChannel),
			any(ProducerProperties.class));
		verify(delegate).unbind();
		binderFactory.destroy();
		scheduler.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	void bindingAutostartup() throws Exception {
		ApplicationContext context = new SpringApplicationBuilder(FooConfiguration.class)
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input-in-0.consumer.auto-startup=false");
		BindingService bindingService = context.getBean(BindingService.class);

		Field cbField = ReflectionUtils.findField(BindingService.class,
			"consumerBindings");
		cbField.setAccessible(true);
		Map<String, Object> cbMap = (Map<String, Object>) cbField.get(bindingService);
		Binding<?> inputBinding = ((List<Binding<?>>) cbMap.get("input-in-0")).get(0);
		assertThat(inputBinding.isRunning()).isFalse();
	}

	@Test
	void bindingNameAsTopLevelProperty() {
		ApplicationContext context = new SpringApplicationBuilder(BarConfiguration.class)
			.web(WebApplicationType.NONE).run();

		final BindingServiceProperties bindingServiceProperties = context.getBean(BindingServiceProperties.class);

		final ConsumerProperties consumerProperties = bindingServiceProperties.getConsumerProperties("myFunction-in-0");
		assertThat(consumerProperties.getBindingName()).isEqualTo("myFunction-in-0");
		final ProducerProperties producerProperties = bindingServiceProperties.getProducerProperties("myFunction-out-0");
		assertThat(producerProperties.getBindingName()).isEqualTo("myFunction-out-0");
	}

	private DefaultBinderFactory createMockBinderFactory() {
		BinderTypeRegistry binderTypeRegistry = createMockBinderTypeRegistry();
		return new DefaultBinderFactory(
			Collections.singletonMap("mock",
				new BinderConfiguration("mock", new HashMap<>(), true, true)),
			binderTypeRegistry, null);
	}

	private DefaultBinderTypeRegistry createMockBinderTypeRegistry() {
		return new DefaultBinderTypeRegistry(Collections.singletonMap("mock",
			new BinderType("mock", new Class[] { IntegrationTestsMockBinderConfiguration.class })));
	}

	private BindingServiceProperties createBindingServiceProperties(
		HashMap<String, String> properties) {
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		org.springframework.boot.context.properties.bind.Binder propertiesBinder;
		propertiesBinder = new org.springframework.boot.context.properties.bind.Binder(
			new MapConfigurationPropertySource(properties));
		propertiesBinder.bind("spring.cloud.stream",
			org.springframework.boot.context.properties.bind.Bindable
				.ofInstance(bindingServiceProperties));
		return bindingServiceProperties;
	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class DefaultConsumerPropertiesTestSink {

	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class FooConfiguration {

		@Bean("input")
		public Consumer<Message<?>> log() {
			return System.out::println;
		}

	}

	@EnableTestBinder
	@EnableAutoConfiguration
	public static class BarConfiguration {

		@Bean
		public Function<String, String> myFunction() {
			return s -> s;
		}

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
