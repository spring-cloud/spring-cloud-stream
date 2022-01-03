/*
 * Copyright 2013-2018 the original author or authors.
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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.springframework.cloud.stream.binder.AbstractBinderTests.Station.Readings;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.StreamListenerMessageHandler;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @author Jacob Severson
 * @author Artem Bilan
 */
// @checkstyle:off
@SuppressWarnings("unchecked")
public abstract class AbstractBinderTests<B extends AbstractTestBinder<? extends AbstractBinder<MessageChannel, CP, PP>, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties> {

	// @checkstyle:on

	protected final Log logger = LogFactory.getLog(this.getClass());

	protected B testBinder;

	protected SmartMessageConverter messageConverter;

	protected GenericApplicationContext applicationContext;

	/**
	 * Subclasses may override this default value to have tests wait longer for a message
	 * receive, for example if running in an environment that is known to be slow (e.g.
	 * travis).
	 */
	protected double timeoutMultiplier = 1.0D;

	@BeforeEach
	public void before() {
		applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		this.messageConverter = new CompositeMessageConverterFactory()
				.getMessageConverterForAllRegistered();
	}

	/**
	 * Attempt to receive a message on the given channel, waiting up to 1s (times the
	 * {@link #timeoutMultiplier}).
	 */
	protected Message<?> receive(PollableChannel channel) {
		return receive(channel, 1);
	}

	/**
	 * Attempt to receive a message on the given channel, waiting up to 1s *
	 * additionalMultiplier * {@link #timeoutMultiplier}).
	 *
	 * Allows accomodating tests which are slower than normal (e.g. retry).
	 */
	protected Message<?> receive(PollableChannel channel, int additionalMultiplier) {
		long startTime = System.currentTimeMillis();
		Message<?> receive = channel
				.receive((int) (1000 * this.timeoutMultiplier * additionalMultiplier));
		long elapsed = System.currentTimeMillis() - startTime;
		this.logger.debug("receive() took " + elapsed / 1000 + " seconds");
		return receive;
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testClean(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();
		Binding<MessageChannel> foo0ProducerBinding = binder.bindProducer(
				String.format("foo%s0", getDestinationNameDelimiter()),
				this.createBindableChannel("output", new BindingProperties()),
				createProducerProperties(testInfo));
		Binding<MessageChannel> foo0ConsumerBinding = binder.bindConsumer(
				String.format("foo%s0", getDestinationNameDelimiter()), "testClean",
				this.createBindableChannel("input", new BindingProperties()),
				createConsumerProperties());
		Binding<MessageChannel> foo1ProducerBinding = binder.bindProducer(
				String.format("foo%s1", getDestinationNameDelimiter()),
				this.createBindableChannel("output", new BindingProperties()),
				createProducerProperties(testInfo));
		Binding<MessageChannel> foo1ConsumerBinding = binder.bindConsumer(
				String.format("foo%s1", getDestinationNameDelimiter()), "testClean",
				this.createBindableChannel("input", new BindingProperties()),
				createConsumerProperties());
		Binding<MessageChannel> foo2ProducerBinding = binder.bindProducer(
				String.format("foo%s2", getDestinationNameDelimiter()),
				this.createBindableChannel("output", new BindingProperties()),
				createProducerProperties(testInfo));
		foo0ProducerBinding.unbind();
		assertThat(TestUtils
				.getPropertyValue(foo0ProducerBinding, "lifecycle", Lifecycle.class)
				.isRunning()).isFalse();
		foo0ConsumerBinding.unbind();
		foo1ProducerBinding.unbind();
		assertThat(TestUtils
				.getPropertyValue(foo0ConsumerBinding, "lifecycle", Lifecycle.class)
				.isRunning()).isFalse();
		assertThat(TestUtils
				.getPropertyValue(foo1ProducerBinding, "lifecycle", Lifecycle.class)
				.isRunning()).isFalse();
		foo1ConsumerBinding.unbind();
		foo2ProducerBinding.unbind();
		assertThat(TestUtils
				.getPropertyValue(foo1ConsumerBinding, "lifecycle", Lifecycle.class)
				.isRunning()).isFalse();
		assertThat(TestUtils
				.getPropertyValue(foo2ProducerBinding, "lifecycle", Lifecycle.class)
				.isRunning()).isFalse();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendAndReceive(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				outputBindingProperties);

		BindingProperties inputBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				inputBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("foo%s0", getDestinationNameDelimiter()),
				moduleOutputChannel, outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("foo%s0", getDestinationNameDelimiter()),
				"testSendAndReceive", moduleInputChannel,
				inputBindingProperties.getConsumer());
		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain").build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<Message<byte[]>>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get().getPayload()).isEqualTo("foo".getBytes());
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE)
				.toString()).isEqualTo("text/plain");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testSendAndReceiveMultipleTopics(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));

		DirectChannel moduleOutputChannel1 = createBindableChannel("output1",
				producerBindingProperties);

		DirectChannel moduleOutputChannel2 = createBindableChannel("output2",
				producerBindingProperties);

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer(
				String.format("foo%sxy", getDestinationNameDelimiter()),
				moduleOutputChannel1, producerBindingProperties.getProducer());
		Binding<MessageChannel> producerBinding2 = binder.bindProducer(
				String.format("foo%syz",

						getDestinationNameDelimiter()),
				moduleOutputChannel2, producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(
				String.format("foo%sxy", getDestinationNameDelimiter()),
				"testSendAndReceiveMultipleTopics", moduleInputChannel,
				createConsumerProperties());
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(
				String.format("foo%syz", getDestinationNameDelimiter()),
				"testSendAndReceiveMultipleTopics", moduleInputChannel,
				createConsumerProperties());

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = MessageBuilder.withPayload(testPayload1.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.APPLICATION_OCTET_STREAM)
				.build();
		String testPayload2 = "foo" + UUID.randomUUID().toString();
		Message<?> message2 = MessageBuilder.withPayload(testPayload2.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.APPLICATION_OCTET_STREAM)
				.build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message1);
		moduleOutputChannel2.send(message2);

		Message<?>[] messages = new Message[2];
		messages[0] = receive(moduleInputChannel);
		messages[1] = receive(moduleInputChannel);

		assertThat(messages[0]).isNotNull();
		assertThat(messages[1]).isNotNull();
		assertThat(messages).extracting("payload").containsExactlyInAnyOrder(
				testPayload1.getBytes(), testPayload2.getBytes());

		producerBinding1.unbind();
		producerBinding2.unbind();

		consumerBinding1.unbind();
		consumerBinding2.unbind();
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testSendAndReceiveNoOriginalContentType(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);
		BindingProperties inputBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				inputBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bar%s0", getDestinationNameDelimiter()),
				moduleOutputChannel, producerBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bar%s0", getDestinationNameDelimiter()),
				"testSendAndReceiveNoOriginalContentType", moduleInputChannel,
				createConsumerProperties());
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<Message<byte[]>>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");
		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(inboundMessageRef.get().getPayload()).isEqualTo("foo".getBytes());
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE)
				.toString()).isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	protected abstract B getBinder() throws Exception;

	protected abstract CP createConsumerProperties();

	protected abstract PP createProducerProperties(TestInfo testInfo);

	protected final BindingProperties createConsumerBindingProperties(
			CP consumerProperties) {
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setConsumer(consumerProperties);
		return bindingProperties;
	}

	protected BindingProperties createProducerBindingProperties(PP producerProperties) {
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);
		return bindingProperties;
	}

	protected DirectChannel createBindableChannel(String channelName,
			BindingProperties bindingProperties) throws Exception {
		// The 'channelName.contains("input")' is strictly for convenience to avoid
		// modifications in multiple tests
		return this.createBindableChannel(channelName, bindingProperties,
				channelName.contains("input"));
	}

	protected DirectChannel createBindableChannel(String channelName,
			BindingProperties bindingProperties, boolean inputChannel) throws Exception {
		MessageConverterConfigurer messageConverterConfigurer = createConverterConfigurer(
				channelName, bindingProperties);
		DirectChannel channel = new DirectChannel();
		channel.setBeanName(channelName);
		if (inputChannel) {
			messageConverterConfigurer.configureInputChannel(channel, channelName);
		}
		else {
			messageConverterConfigurer.configureOutputChannel(channel, channelName);
		}
		return channel;
	}

	protected DefaultPollableMessageSource createBindableMessageSource(String bindingName,
			BindingProperties bindingProperties) throws Exception {
		DefaultPollableMessageSource source = new DefaultPollableMessageSource(
				new CompositeMessageConverterFactory()
						.getMessageConverterForAllRegistered());
		createConverterConfigurer(bindingName, bindingProperties)
				.configurePolledMessageSource(source, bindingName);
		return source;
	}

	private MessageConverterConfigurer createConverterConfigurer(String channelName,
			BindingProperties bindingProperties) throws Exception {
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		bindingServiceProperties.getBindings().put(channelName, bindingProperties);
		bindingServiceProperties.setApplicationContext(applicationContext);
		bindingServiceProperties.setConversionService(new DefaultConversionService());
		bindingServiceProperties.afterPropertiesSet();
		MessageConverterConfigurer messageConverterConfigurer = new MessageConverterConfigurer(
				bindingServiceProperties,
				new CompositeMessageConverterFactory(null, null).getMessageConverterForAllRegistered());
		messageConverterConfigurer.setBeanFactory(applicationContext.getBeanFactory());
		return messageConverterConfigurer;
	}

	@AfterEach
	public void cleanup() {
		if (this.testBinder != null) {
			this.testBinder.cleanup();
		}
	}

	/**
	 * If appropriate, let the binder middleware settle down a bit while binding/unbinding
	 * actually happens.
	 */
	protected void binderBindUnbindLatency() throws InterruptedException {
		// default none
	}

	/**
	 * Create a new spy on the given 'queue'. This allows de-correlating the creation of
	 * the 'connection' from its actual usage, which may be needed by some implementations
	 * to see messages sent after connection creation.
	 */
	public abstract Spy spyOn(String name);

	/**
	 * Set the delimiter that will be used in the message source/target name. Some brokers
	 * may have naming constraints (such as SQS), so this provides a way to override the
	 * character being used as a delimiter. The default is a period.
	 */
	protected String getDestinationNameDelimiter() {
		return ".";
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendPojoReceivePojoWithStreamListenerDefaultContentType(TestInfo testInfo)
			throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);

		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		BindingProperties consumerBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());

		DirectChannel moduleInputChannel = createBindableChannel("input",
				consumerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bad%s0a", getDestinationNameDelimiter()),
				moduleOutputChannel, producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bad%s0a", getDestinationNameDelimiter()), "test-1",
				moduleInputChannel, consumerBindingProperties.getConsumer());

		Station station = new Station();
		Message<?> message = MessageBuilder.withPayload(station).build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel replyChannel = (QueueChannel) handler.getOutputChannel();

		Message<?> replyMessage = replyChannel.receive(5000);
		assertThat(replyMessage.getPayload() instanceof Station).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendJsonReceivePojoWithStreamListener(TestInfo testInfo) throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		BindingProperties consumerBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());

		DirectChannel moduleInputChannel = createBindableChannel("input",
				consumerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bad%s0d", getDestinationNameDelimiter()),
				moduleOutputChannel, producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bad%s0d", getDestinationNameDelimiter()), "test-4",
				moduleInputChannel, consumerBindingProperties.getConsumer());

		String value = "{\"readings\":[{\"stationid\":\"fgh\","
				+ "\"customerid\":\"12345\",\"timestamp\":null},"
				+ "{\"stationid\":\"hjk\",\"customerid\":\"222\",\"timestamp\":null}]}";

		Message<?> message = MessageBuilder.withPayload(value)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
				.build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel channel = (QueueChannel) handler.getOutputChannel();

		Message<Station> reply = (Message<Station>) channel.receive(5000);

		assertThat(reply).isNotNull();
		assertThat(reply.getPayload() instanceof Station).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendJsonReceiveJsonWithStreamListener(TestInfo testInfo) throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStationString", String.class);
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		BindingProperties consumerBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());

		DirectChannel moduleInputChannel = createBindableChannel("input",
				consumerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bad%s0e", getDestinationNameDelimiter()),
				moduleOutputChannel, producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bad%s0e", getDestinationNameDelimiter()), "test-5",
				moduleInputChannel, consumerBindingProperties.getConsumer());

		String value = "{\"readings\":[{\"stationid\":\"fgh\","
				+ "\"customerid\":\"12345\",\"timestamp\":null},"
				+ "{\"stationid\":\"hjk\",\"customerid\":\"222\",\"timestamp\":null}]}";

		Message<?> message = MessageBuilder.withPayload(value)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
				.build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel channel = (QueueChannel) handler.getOutputChannel();

		Message<String> reply = (Message<String>) channel.receive(5000);

		assertThat(reply).isNotNull();
		assertThat(reply.getPayload() instanceof String).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendPojoReceivePojoWithStreamListener(TestInfo testInfo) throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		BindingProperties consumerBindingProperties = createConsumerBindingProperties(
				createConsumerProperties());

		DirectChannel moduleInputChannel = createBindableChannel("input",
				consumerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bad%s0f", getDestinationNameDelimiter()),
				moduleOutputChannel, producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bad%s0f", getDestinationNameDelimiter()), "test-6",
				moduleInputChannel, consumerBindingProperties.getConsumer());

		Readings r1 = new Readings();
		r1.setCustomerid("123");
		r1.setStationid("XYZ");
		Readings r2 = new Readings();
		r2.setCustomerid("546");
		r2.setStationid("ABC");
		Station station = new Station();
		station.setReadings(Arrays.asList(r1, r2));
		Message<?> message = MessageBuilder.withPayload(station)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
				.build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel channel = (QueueChannel) handler.getOutputChannel();

		Message<Station> reply = (Message<Station>) channel.receive(5000);

		assertThat(reply).isNotNull();
		assertThat(reply.getPayload() instanceof Station).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("unused") // it is used via reflection
	private Station echoStation(Station station) {
		return station;
	}

	@SuppressWarnings("unused") // it is used via reflection
	private String echoStationString(String station) {
		return station;
	}

	private StreamListenerMessageHandler buildStreamListener(Class<?> handlerClass,
			String handlerMethodName, Class<?>... parameters) throws Exception {
		String channelName = "reply_" + System.nanoTime();

		this.applicationContext.getBeanFactory().registerSingleton(channelName, new QueueChannel());

		Method m = ReflectionUtils.findMethod(handlerClass, handlerMethodName,
				parameters);
		InvocableHandlerMethod method = new InvocableHandlerMethod(this, m);
		HandlerMethodArgumentResolverComposite resolver = new HandlerMethodArgumentResolverComposite();
		CompositeMessageConverterFactory factory = new CompositeMessageConverterFactory();
		resolver.addResolver(new PayloadMethodArgumentResolver(
				factory.getMessageConverterForAllRegistered()));
		method.setMessageMethodArgumentResolvers(resolver);
		Constructor<?> c = ReflectionUtils.accessibleConstructor(
				StreamListenerMessageHandler.class, InvocableHandlerMethod.class,
				boolean.class, String[].class);
		StreamListenerMessageHandler handler = (StreamListenerMessageHandler) c
				.newInstance(method, false, new String[] {});
		handler.setOutputChannelName(channelName);
		handler.setBeanFactory(this.applicationContext);
		handler.afterPropertiesSet();
//		context.refresh();
		return handler;
	}

	public static class Station {

		List<Readings> readings = new ArrayList<>();

		public List<Readings> getReadings() {
			return this.readings;
		}

		public void setReadings(List<Readings> readings) {
			this.readings = readings;
		}

		@SuppressWarnings("serial")
		public static class Readings implements Serializable {

			public String stationid;

			public String customerid;

			public String timestamp;

			public String getStationid() {
				return this.stationid;
			}

			public void setStationid(String stationid) {
				this.stationid = stationid;
			}

			public String getCustomerid() {
				return this.customerid;
			}

			public void setCustomerid(String customerid) {
				this.customerid = customerid;
			}

			public String getTimestamp() {
				return this.timestamp;
			}

			public void setTimestamp(String timestamp) {
				this.timestamp = timestamp;
			}

		}

	}

}
