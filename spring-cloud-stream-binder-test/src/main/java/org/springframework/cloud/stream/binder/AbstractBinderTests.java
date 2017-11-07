/*
 * Copyright 2013-2017 the original author or authors.
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
import org.junit.After;
import org.junit.Test;

import org.springframework.cloud.stream.binder.AbstractBinderTests.Station.Readings;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.StreamListenerMessageHandler;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
@SuppressWarnings("unchecked")
public abstract class AbstractBinderTests<B extends AbstractTestBinder<? extends AbstractBinder<MessageChannel, CP, PP>, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	protected B testBinder;

	/**
	 * Subclasses may override this default value to have tests wait longer for a message
	 * receive, for example if running in an environment that is known to be slow (e.g.
	 * travis).
	 */
	protected double timeoutMultiplier = 1.0D;

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
				.receive((int) (1000 * timeoutMultiplier * additionalMultiplier));
		long elapsed = System.currentTimeMillis() - startTime;
		logger.debug("receive() took " + elapsed / 1000 + " seconds");
		return receive;
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testClean() throws Exception {
		Binder binder = getBinder();
		Binding<MessageChannel> foo0ProducerBinding = binder.bindProducer("foo.0",
				this.createBindableChannel("output", new BindingProperties()), createProducerProperties());
		Binding<MessageChannel> foo0ConsumerBinding = binder.bindConsumer("foo.0",
				"testClean", this.createBindableChannel("input", new BindingProperties()), createConsumerProperties());
		Binding<MessageChannel> foo1ProducerBinding = binder.bindProducer("foo.1",
				this.createBindableChannel("output", new BindingProperties()), createProducerProperties());
		Binding<MessageChannel> foo1ConsumerBinding = binder.bindConsumer("foo.1",
				"testClean", this.createBindableChannel("input", new BindingProperties()), createConsumerProperties());
		Binding<MessageChannel> foo2ProducerBinding = binder.bindProducer("foo.2",
				this.createBindableChannel("output", new BindingProperties()), createProducerProperties());
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
	public void testSendAndReceive() throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", outputBindingProperties);

		BindingProperties inputBindingProperties = createConsumerBindingProperties(createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", inputBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
				outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "testSendAndReceive", moduleInputChannel,
				createConsumerProperties());
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar")
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<String>> inboundMessageRef = new AtomicReference<Message<String>>();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				try {
					inboundMessageRef.set((Message<String>) message);
				}
				finally {
					latch.countDown();
				}
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get().getPayload()).isEqualTo("foo");
		assertThat(inboundMessageRef.get().getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE).toString()).isEqualTo("foo/bar");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	private class Foo {
		private String name;

		@SuppressWarnings("unused")
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendAndReceiveKryo() throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", outputBindingProperties);

		BindingProperties inputBindingProperties = createConsumerBindingProperties(createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", inputBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
				outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "testSendAndReceiveKryo", moduleInputChannel,
				createConsumerProperties());
		Foo foo = new Foo();
		foo.setName("Bill");
		Message<?> message = MessageBuilder.withPayload(foo).setHeader(MessageHeaders.CONTENT_TYPE, MessageConverterUtils.X_JAVA_OBJECT)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<Foo>> inboundMessageRef = new AtomicReference<Message<Foo>>();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				try {
					inboundMessageRef.set((Message<Foo>) message);
				}
				finally {
					latch.countDown();
				}
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get().getPayload()).isInstanceOf(Foo.class);
		assertThat(inboundMessageRef.get().getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertTrue(equalTypeAndSubType((MimeType)inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE), MessageConverterUtils.X_JAVA_OBJECT));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendAndReceiveJavaSerialization() throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", outputBindingProperties);

		BindingProperties inputBindingProperties = createConsumerBindingProperties(createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", inputBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
				outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "testSendAndReceiveJavaSerialization", moduleInputChannel,
				createConsumerProperties());
		SerializableFoo foo = new SerializableFoo();
		Message<?> message = MessageBuilder.withPayload(foo).setHeader(MessageHeaders.CONTENT_TYPE, MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<SerializableFoo>> inboundMessageRef = new AtomicReference<Message<SerializableFoo>>();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				try {
					inboundMessageRef.set((Message<SerializableFoo>) message);
				}
				finally {
					latch.countDown();
				}
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get().getPayload()).isInstanceOf(SerializableFoo.class);
		assertThat(inboundMessageRef.get().getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testSendAndReceiveMultipleTopics() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel1 = createBindableChannel("output1",
				createProducerBindingProperties(createProducerProperties()));
		DirectChannel moduleOutputChannel2 = createBindableChannel("output2",
				createProducerBindingProperties(createProducerProperties()));

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer("foo.x",
				moduleOutputChannel1, createProducerProperties());
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("foo.y",
				moduleOutputChannel2, createProducerProperties());

		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer("foo.x",
				"testSendAndReceiveMultipleTopics", moduleInputChannel,
				createConsumerProperties());
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer("foo.y",
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
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);
		BindingProperties inputBindingProperties = createConsumerBindingProperties(createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", inputBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar.0",
				moduleOutputChannel, producerBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bar.0",
				"testSendAndReceiveNoOriginalContentType", moduleInputChannel,
				createConsumerProperties());
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<String>> inboundMessageRef = new AtomicReference<Message<String>>();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				try {
					inboundMessageRef.set((Message<String>) message);
				}
				finally {
					latch.countDown();
				}
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");
		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(inboundMessageRef.get().getPayload()).isEqualTo("foo");
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE).toString())
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	protected abstract B getBinder() throws Exception;

	protected abstract CP createConsumerProperties();

	protected abstract PP createProducerProperties();

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
		// The 'channelName.contains("input")' is strictly for convenience to avoid modifications in multiple tests
		return this.createBindableChannel(channelName, bindingProperties, channelName.contains("input"));
	}

	protected DirectChannel createBindableChannel(String channelName,
			BindingProperties bindingProperties, boolean inputChannel) throws Exception {
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		bindingServiceProperties.getBindings().put(channelName, bindingProperties);
		ConfigurableApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		bindingServiceProperties.setApplicationContext(applicationContext);
		bindingServiceProperties.setConversionService(new DefaultConversionService());
		bindingServiceProperties.afterPropertiesSet();
		DirectChannel channel = new DirectChannel();
		channel.setBeanName(channelName);
		MessageConverterConfigurer messageConverterConfigurer = new MessageConverterConfigurer(
				bindingServiceProperties,
				new CompositeMessageConverterFactory(null, null));
		messageConverterConfigurer.setBeanFactory(applicationContext.getBeanFactory());
		messageConverterConfigurer.afterPropertiesSet();
		if (inputChannel){
			messageConverterConfigurer.configureInputChannel(channel, channelName);
		}
		else {
			messageConverterConfigurer.configureOutputChannel(channel, channelName);
		}
		return channel;
	}

	@After
	public void cleanup() {
		if (testBinder != null) {
			testBinder.cleanup();
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
	public abstract Spy spyOn(final String name);


	@SuppressWarnings("rawtypes")
	@Test
	public void testSendPojoReceivePojoWithStreamListenerDefaultContentType()
			throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);

		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());

		Station station = new Station();
		Message<?> message = MessageBuilder.withPayload(station).build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel replyChannel = (QueueChannel) handler.getOutputChannel();

		Message<?> replyMessage = replyChannel.receive(5000);
		assertTrue(replyMessage.getPayload() instanceof Station);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendPojoReceivePojoKryoWithStreamListener() throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);

		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());

		Station station = new Station();
		Message<?> message = MessageBuilder.withPayload(station).setHeader(
				MessageHeaders.CONTENT_TYPE, MessageConverterUtils.X_JAVA_OBJECT).build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel replyChannel = (QueueChannel) handler.getOutputChannel();

		Message<?> replyMessage = replyChannel.receive(5000);
		assertTrue(replyMessage.getPayload() instanceof Station);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = MessageHandlingException.class)
	public void testStreamListenerJavaSerializationNonSerializable() throws Exception {
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());
		try {
			Station station = new Station();
			Message<?> message = MessageBuilder.withPayload(station)
					.setHeader(MessageHeaders.CONTENT_TYPE,
							MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT)
					.build();
			moduleOutputChannel.send(message);
		}
		finally {
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendJsonReceivePojoWithStreamListener() throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());

		String value = "{\"readings\":[{\"stationid\":\"fgh\","
				+ "\"customerid\":\"12345\",\"timestamp\":null},{\"stationid\":\"hjk\",\"customerid\":\"222\",\"timestamp\":null}]}";

		Message<?> message = MessageBuilder.withPayload(value)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
				.build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel channel = (QueueChannel) handler.getOutputChannel();

		Message<Station> reply = (Message<Station>) channel.receive(5000);

		assertNotNull(reply);
		assertTrue(reply.getPayload() instanceof Station);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendJsonReceiveJsonWithStreamListener() throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStationString", String.class);
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());

		String value = "{\"readings\":[{\"stationid\":\"fgh\","
				+ "\"customerid\":\"12345\",\"timestamp\":null},{\"stationid\":\"hjk\",\"customerid\":\"222\",\"timestamp\":null}]}";

		Message<?> message = MessageBuilder.withPayload(value)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
				.build();
		moduleInputChannel.subscribe(handler);
		moduleOutputChannel.send(message);

		QueueChannel channel = (QueueChannel) handler.getOutputChannel();

		Message<String> reply = (Message<String>) channel.receive(5000);

		assertNotNull(reply);
		assertTrue(reply.getPayload() instanceof String);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSendPojoReceivePojoWithStreamListener() throws Exception {
		StreamListenerMessageHandler handler = this.buildStreamListener(
				AbstractBinderTests.class, "echoStation", Station.class);
		Binder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input",
				new BindingProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("bad.0",
				moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bad.0", "test",
				moduleInputChannel, createConsumerProperties());

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

		assertNotNull(reply);
		assertTrue(reply.getPayload() instanceof Station);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	private static boolean equalTypeAndSubType(MimeType m1, MimeType m2) {
		return m1 != null && m2 != null && m1.getType().equalsIgnoreCase(m2.getType())
				&& m1.getSubtype().equalsIgnoreCase(m2.getSubtype());
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
		GenericApplicationContext context = new GenericApplicationContext();
		context.getBeanFactory().registerSingleton(channelName, new QueueChannel());

		Method m = ReflectionUtils.findMethod(handlerClass, handlerMethodName,
				parameters);
		InvocableHandlerMethod method = new InvocableHandlerMethod(this, m);
		HandlerMethodArgumentResolverComposite resolver = new HandlerMethodArgumentResolverComposite();
		CompositeMessageConverterFactory factory = new CompositeMessageConverterFactory();
		resolver.addResolver(new PayloadArgumentResolver(
				factory.getMessageConverterForAllRegistered()));
		method.setMessageMethodArgumentResolvers(resolver);
		Constructor<?> c = ReflectionUtils.accessibleConstructor(
				StreamListenerMessageHandler.class, InvocableHandlerMethod.class,
				boolean.class, String[].class);
		StreamListenerMessageHandler handler = (StreamListenerMessageHandler) c
				.newInstance(method, false, new String[] {});
		handler.setOutputChannelName(channelName);
		handler.setBeanFactory(context);
		handler.afterPropertiesSet();
		context.refresh();
		return handler;
	}

	public static class Station {

		List<Readings> readings = new ArrayList<>();

		public List<Readings> getReadings() {
			return readings;
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
				return stationid;
			}

			public void setStationid(String stationid) {
				this.stationid = stationid;
			}

			public String getCustomerid() {
				return customerid;
			}

			public void setCustomerid(String customerid) {
				this.customerid = customerid;
			}

			public String getTimestamp() {
				return timestamp;
			}

			public void setTimestamp(String timestamp) {
				this.timestamp = timestamp;
			}
		}
	}
}
