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

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.ConfigurableApplicationContext;
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
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Mark Fisher
 * @author Marius Bogoevici
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
		Message<?> receive = channel.receive((int) (1000 * timeoutMultiplier * additionalMultiplier));
		long elapsed = System.currentTimeMillis() - startTime;
		logger.debug("receive() took " + elapsed / 1000 + " seconds");
		return receive;
	}

	@Test
	public void testClean() throws Exception {
		Binder binder = getBinder();
		Binding<MessageChannel> foo0ProducerBinding = binder.bindProducer("foo.0", new DirectChannel(),
				createProducerProperties());
		Binding<MessageChannel> foo0ConsumerBinding = binder.bindConsumer("foo.0", "testClean", new DirectChannel(),
				createConsumerProperties());
		Binding<MessageChannel> foo1ProducerBinding = binder.bindProducer("foo.1", new DirectChannel(),
				createProducerProperties());
		Binding<MessageChannel> foo1ConsumerBinding = binder.bindConsumer("foo.1", "testClean", new DirectChannel(),
				createConsumerProperties());
		Binding<MessageChannel> foo2ProducerBinding = binder.bindProducer("foo.2", new DirectChannel(),
				createProducerProperties());
		foo0ProducerBinding.unbind();
		assertThat(TestUtils.getPropertyValue(foo0ProducerBinding, "lifecycle", Lifecycle.class).isRunning())
				.isFalse();
		foo0ConsumerBinding.unbind();
		foo1ProducerBinding.unbind();
		assertThat(TestUtils.getPropertyValue(foo0ConsumerBinding, "lifecycle", Lifecycle.class).isRunning())
				.isFalse();
		assertThat(TestUtils.getPropertyValue(foo1ProducerBinding, "lifecycle", Lifecycle.class).isRunning())
				.isFalse();
		foo1ConsumerBinding.unbind();
		foo2ProducerBinding.unbind();
		assertThat(TestUtils.getPropertyValue(foo1ConsumerBinding, "lifecycle", Lifecycle.class).isRunning())
				.isFalse();
		assertThat(TestUtils.getPropertyValue(foo2ProducerBinding, "lifecycle", Lifecycle.class).isRunning())
				.isFalse();
	}

	@Test
	public void testSendAndReceive() throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", outputBindingProperties);
		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
				outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "testSendAndReceive", moduleInputChannel,
				createConsumerProperties());
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar")
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat(inbound.getPayload()).isEqualTo("foo");
		assertThat(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("foo/bar");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testSendAndReceiveMultipleTopics() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel1 = createBindableChannel("output1",
				createProducerBindingProperties(createProducerProperties()));
		DirectChannel moduleOutputChannel2 = createBindableChannel("output2",
				createProducerBindingProperties(createProducerProperties()));

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer("foo.x", moduleOutputChannel1,
				createProducerProperties());
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("foo.y", moduleOutputChannel2,
				createProducerProperties());

		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer("foo.x", "testSendAndReceiveMultipleTopics", moduleInputChannel,
				createConsumerProperties());
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer("foo.y", "testSendAndReceiveMultipleTopics", moduleInputChannel,
				createConsumerProperties());

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = MessageBuilder.withPayload(testPayload1.getBytes()).build();
		String testPayload2 = "foo" + UUID.randomUUID().toString();
		Message<?> message2 = MessageBuilder.withPayload(testPayload2.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message1);
		moduleOutputChannel2.send(message2);

		Message<?>[] messages = new Message[2];
		messages[0] = receive(moduleInputChannel);
		messages[1] = receive(moduleInputChannel);

		assertThat(messages[0]).isNotNull();
		assertThat(messages[1]).isNotNull();
		assertThat(messages).extracting("payload").containsExactlyInAnyOrder(testPayload1.getBytes(),
				testPayload2.getBytes());

		producerBinding1.unbind();
		producerBinding2.unbind();

		consumerBinding1.unbind();
		consumerBinding2.unbind();
	}

	@Test
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", producerBindingProperties);
		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar.0", moduleOutputChannel,
				producerBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bar.0", "testSendAndReceiveNoOriginalContentType", moduleInputChannel,
				createConsumerProperties());
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo").build();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat(inbound.getPayload()).isEqualTo("foo");
		assertThat(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	protected abstract B getBinder() throws Exception;

	protected abstract CP createConsumerProperties();

	protected abstract PP createProducerProperties();

	protected final BindingProperties createConsumerBindingProperties(CP consumerProperties) {
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setConsumer(consumerProperties);
		return bindingProperties;
	}

	protected BindingProperties createProducerBindingProperties(PP producerProperties) {
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);
		return bindingProperties;
	}

	protected DirectChannel createBindableChannel(String channelName, BindingProperties bindingProperties)
			throws Exception {
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
		messageConverterConfigurer.configureOutputChannel(channel, channelName);
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
}
