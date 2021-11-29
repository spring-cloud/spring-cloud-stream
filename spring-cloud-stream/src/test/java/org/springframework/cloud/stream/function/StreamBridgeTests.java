/*
 * Copyright 2020-2021 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver.NewDestinationBindingCallback;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 */
@SuppressWarnings("deprecation")
public class StreamBridgeTests {

	@Before
	public void before() {
		System.clearProperty("spring.cloud.function.definition");
	}

	@Test
	public void testWithOutputContentTypeWildCardBindings() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class, EmptyConfigurationWithCustomConverters.class))
						.web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.bindings.foo.content-type=application/*+foo  ",
								"--spring.cloud.stream.bindings.bar.content-type=application/*+non-registered-foo",
								"--spring.jmx.enabled=false")) {
			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo", "hello foo");
			bridge.send("bar", "hello bar");

			OutputDestination output = context.getBean(OutputDestination.class);

			assertThat(output.receive(1000, "foo").getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeType.valueOf("application/json+foo"));
			assertThat(output.receive(1000, "bar").getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeType.valueOf("application/blahblah+non-registered-foo"));

		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNoCachingOfStreamBridgeFunction() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class, InterceptorConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.cloud.function.definition=function",
								"--spring.jmx.enabled=false")) {
			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("function-in-0", (Object) "hello foo", MimeTypeUtils.TEXT_PLAIN);
			bridge.send("function-in-0", (Object) "hello foo", MimeTypeUtils.APPLICATION_JSON);
			bridge.send("function-in-0", (Object) "hello foo", MimeTypeUtils.TEXT_HTML);

			Field field = ReflectionUtils.findField(StreamBridge.class, "streamBridgeFunctionCache");
			field.setAccessible(true);
			Map<String, FunctionInvocationWrapper> map = (Map<String, FunctionInvocationWrapper>) field.get(bridge);
			assertThat(map.size()).isEqualTo(3);
		}
	}

	@Test
	public void testDelayedSend() {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class, EmptyConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			executor.schedule(() -> bridge.send("blah", "hello foo"), 5000, TimeUnit.MILLISECONDS);

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(10000, "blah");
			assertThat(message).isNotNull();
			assertThat(new String(message.getPayload())).isEqualTo("hello foo");
		}
		finally {
			executor.shutdownNow();
		}
	}

	@Test
	public void testWithInterceptorsMatchedAgainstAllPatterns() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class, InterceptorConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.cloud.function.definition=function",
								"--spring.jmx.enabled=false")) {
			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("function-in-0", "hello foo");

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "function-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("hello foo");
			assertThat(message.getHeaders().get("intercepted")).isEqualTo("true");
		}
	}

	@Test
	public void testWithInterceptorsRegisteredOnlyOnOutputChannel() throws InterruptedException {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(GH2180Configuration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.jmx.enabled=false")) {

			MessageChannel inputChannel = context.getBean("inputChannel", MessageChannel.class);
			inputChannel.send(MessageBuilder.withPayload("hello foo").build());

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "outgoing-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("hello foo");
			assertThat(message.getHeaders().get("intercepted")).isEqualTo("true");
			//Ensure that the LoggingHandler in the first SI flow is invoked.
			GH2180Configuration.LATCH1.await(10, TimeUnit.SECONDS);
			//Ensure that the second SI flow does not trigger its LoggingHandler (aka wiretap/interceptor).
			assertThat(GH2180Configuration.LATCH2.getCount()).isEqualTo(1);
		}
	}

	@Test
	public void testBindingPropertiesAreHonored() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.cloud.function.definition=consumer;function",
								"--spring.jmx.enabled=false",
								"--spring.cloud.stream.bindings.foo.destination=function-in-0",
								"--spring.cloud.stream.bindings.foo.producer.partitionCount=5",
								"--spring.cloud.stream.bindings.foo.consumer.concurrency=2")) {

			BindingServiceProperties bsProperties = context.getBean(BindingServiceProperties.class);
			assertThat(bsProperties.getConsumerProperties("foo").getConcurrency()).isEqualTo(2);
			assertThat(bsProperties.getProducerProperties("foo").getPartitionCount()).isEqualTo(5);
			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("consumer-in-0", "hello foo");

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "function-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("hello foo");
			assertThat(message.getHeaders().get("concurrency")).isEqualTo(2);
			assertThat(message.getHeaders().get("partitionCount")).isEqualTo(5);
		}
	}

	//see https://github.com/spring-cloud/spring-cloud-function/issues/573 for more details
	@Test
	public void testBridgeActivationWhenFunctionDefinitionIsPresent() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(SimpleConfiguration.class))
						.web(WebApplicationType.NONE).run(
								"--spring.cloud.function.definition=echo;uppercase",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("echo-in-0", "hello foo");

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			assertThat(new String(outputDestination.receive(100, "echo-out-0").getPayload())).isEqualTo("hello foo");
		}
	}

	@Test(expected = NoSuchBeanDefinitionException.class)
	public void testNoBridgeIfNoSourcePropertyDefined() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration())
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			context.getBean(StreamBridge.class);
			fail();
		}
	}

	@Test
	public void testBridgeFunctions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(EmptyConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=foo;bar",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo-out-0", "hello foo");
			bridge.send("bar-out-0", "hello bar");


			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			assertThat(new String(outputDestination.receive(100, "foo-out-0").getPayload())).isEqualTo("hello foo");
			assertThat(new String(outputDestination.receive(100, "bar-out-0").getPayload())).isEqualTo("hello bar");
		}
	}

	@Test
	public void testBridgeFunctionsSendingMessagePreservingHeaders() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(EmptyConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=foo;bar",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo-out-0", MessageBuilder.withPayload("hello foo").setHeader("foo", "foo").build());
			bridge.send("bar-out-0", MessageBuilder.withPayload("hello bar").setHeader("bar", "bar").build());


			OutputDestination outputDestination = context.getBean(OutputDestination.class);


			Message<?> message = outputDestination.receive(100, "foo-out-0");
			assertThat(message.getPayload()).isEqualTo("hello foo".getBytes());
			assertThat(message.getHeaders().get("foo")).isEqualTo("foo");

			message = outputDestination.receive(100, "bar-out-0");
			assertThat(message.getPayload()).isEqualTo("hello bar".getBytes());
			assertThat(message.getHeaders().get("bar")).isEqualTo("bar");
		}
	}

	@Test
	public void testBridgeFunctionsWitthPartitionInformation() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(EmptyConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=foo;bar",
								"--spring.cloud.stream.bindings.foo-out-0.producer.partitionKeyExpression=payload",
								"--spring.cloud.stream.bindings.foo-out-0.producer.partitionCount=5",
								"--spring.cloud.stream.bindings.bar-out-0.producer.partitionKeyExpression=payload",
								"--spring.cloud.stream.bindings.bar-out-0.producer.partitionCount=1",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo-out-0", "a");
			bridge.send("bar-out-0", "b");
			bridge.send("foo-out-0", "c");
			bridge.send("foo-out-0", "d");
			bridge.send("bar-out-0", "e");
			bridge.send("foo-out-0", "f");
			bridge.send("bar-out-0", "g");


			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "foo-out-0");

			assertThat(new String(message.getPayload())).isEqualTo("a");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(2);

			message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("c");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(4);

			message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("d");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("b");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("e");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("g");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("f");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(2);

			//assertThat(new String(outputDestination.receive(100, "bar-out-0").getPayload())).isEqualTo("b");
		}
	}

	@Test
	public void testSendingMessageToOutputOfExistingSupplier() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(TestConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=supplier;foo",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("supplier-out-0", "blah");
			bridge.send("foo-out-0", "b");


			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("b");
			message = outputDestination.receive(100, "supplier-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("hello");
			message = outputDestination.receive(100, "supplier-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("blah");
		}
	}

	@Test
	public void testDynamicDestination() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(TestConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo-out-0", "b");
			bridge.send("bar", "hello");
			bridge.send("blah", MessageBuilder.withPayload("message").setHeader("foo", "foo").build());

			Message<byte[]> message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("b");

			message = outputDestination.receive(100, "bar");
			assertThat(new String(message.getPayload())).isEqualTo("hello");

			message = outputDestination.receive(100, "blah");
			assertThat(new String(message.getPayload())).isEqualTo("message");
		}
	}

	@Test
	public void testWithIntegrationFlowBecauseMarcinSaidSo() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(IntegrationFlowConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo", "blah");

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "output");
			assertThat(new String(message.getPayload())).isEqualTo("BLAH");
		}
	}

	@Test
	public void testNewBindingCallback() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(BindingCallbackConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=uppercase",
								"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("uppercase-in-0", "hello");
			assertThat(context.getBean("callbackVerifier", AtomicBoolean.class)).isTrue();
		}
	}

	@EnableAutoConfiguration
	public static class EmptyConfiguration {

	}

	@EnableAutoConfiguration
	public static class EmptyConfigurationWithCustomConverters {

		@Bean
		public MessageConverter fooConverter() {
			return new AbstractMessageConverter(MimeType.valueOf("application/json+foo"), MimeType.valueOf("application/json+blah")) {
				@Override
				protected boolean supports(Class<?> clazz) {
					return true;
				}

				@Override
				@Nullable
				protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
					return message.getPayload();
				}

				@Override
				@Nullable
				protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
					if (headers.containsKey(MessageHeaders.CONTENT_TYPE) &&
							(headers.get(MessageHeaders.CONTENT_TYPE).toString().endsWith("+foo") ||
									headers.get(MessageHeaders.CONTENT_TYPE).toString().endsWith("+blah"))) {
						return payload;
					}
					return null;
				}
			};
		}

		@Bean
		public MessageConverter barConverter() {
			return new AbstractMessageConverter(MimeType.valueOf("application/blahblah+non-registered-foo")) {
				@Override
				protected boolean supports(Class<?> clazz) {
					return true;
				}

				@Override
				@Nullable
				protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
					return message.getPayload();
				}

				@Override
				@Nullable
				protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
					if (headers.containsKey(MessageHeaders.CONTENT_TYPE) &&
							(headers.get(MessageHeaders.CONTENT_TYPE).toString().endsWith("+non-registered-foo"))) {
						return payload;
					}
					return null;
				}
			};
		}
	}

	@EnableAutoConfiguration
	public static class ConsumerConfiguration {
		@Bean
		public Consumer<String> consumer(StreamBridge bridge, BindingServiceProperties properties) {
			return v -> {
				BindingServiceProperties p = properties;
				bridge.send("foo", v);
			};
		}
		@Bean
		public Function<String, Message<String>> function(StreamBridge bridge, BindingServiceProperties properties) {
			return v -> {
				int concurrency = properties.getConsumerProperties("foo").getConcurrency();
				int partitionCount = properties.getProducerProperties("foo").getPartitionCount();
				BindingServiceProperties p = properties;
				return MessageBuilder.withPayload(v)
						.setHeader("concurrency", concurrency)
						.setHeader("partitionCount", partitionCount)
						.build();
			};
		}
	}

	@EnableAutoConfiguration
	public static class InterceptorConfiguration {
		@Bean
		@GlobalChannelInterceptor(patterns = "*")
		public ChannelInterceptor interceptor() {
			return new ChannelInterceptor() {
				@Override
				public Message<?> preSend(Message<?> message, MessageChannel channel) {
					return MessageBuilder.fromMessage(message).setHeader("intercepted", "true").build();
				}
			};
		}
	}

	@EnableAutoConfiguration
	public static class TestConfiguration {

		@Bean
		public Supplier<String> supplier() {
			return () -> "hello";
		}
	}

	@EnableAutoConfiguration
	public static class SimpleConfiguration {

		@Bean
		public Function<String, String> echo() {
			return v -> v;
		}

		@Bean
		public Function<String, String> uppercase() {
			return v -> v.toUpperCase();
		}
	}


	@EnableAutoConfiguration
	public static class BindingCallbackConfiguration {

		@Bean
		public Function<String, String> echo() {
			return v -> v;
		}

		@Bean
		public Function<String, String> uppercase() {
			return v -> v.toUpperCase();
		}

		@Bean
		public AtomicBoolean callbackVerifier() {
			return new AtomicBoolean();
		}

		@Bean
		public NewDestinationBindingCallback callback(AtomicBoolean callbackVerifier) {

			return (name, channel, props, extended) -> {
				callbackVerifier.set(true);
			};
		}
	}

	@EnableAutoConfiguration
	public static class IntegrationFlowConfiguration {

		@Bean
		public IntegrationFlow transform(StreamBridge bridge) {
			return IntegrationFlows.from("foo").transform(v -> {
				String s = new String((byte[]) v);
				return s.toUpperCase();
			})
			.handle(v -> bridge.send("output", v))
			.get();
		}
	}

	@EnableAutoConfiguration
	public static class GH2180Configuration {

		static CountDownLatch LATCH1 = new CountDownLatch(1);
		static CountDownLatch LATCH2 = new CountDownLatch(1);

		@Bean
		MessageChannel inputChannel() {
			return new DirectChannel();
		}

		@Bean
		MessageChannel otherInputChannel() {
			return new DirectChannel();
		}

		@Bean
		public IntegrationFlow someFlow(MessageHandler sendMessage, MessageChannel inputChannel) {
			return IntegrationFlows.from(inputChannel)
				.log(LoggingHandler.Level.INFO, (m) -> {
					LATCH1.countDown();
					return "Going through the first flow: " + m.getPayload();
				})
				.handle(sendMessage)
				.get();
		}

		@Bean
		public IntegrationFlow someOtherFlow(MessageHandler sendMessage) {
			return IntegrationFlows.from(otherInputChannel())
				.log(LoggingHandler.Level.INFO, (m) -> {
					LATCH2.countDown();
					return "Going through the second flow: " + m.getPayload();
				})
				.handle(sendMessage)
				.get();
		}

		@Bean
		@GlobalChannelInterceptor(patterns = "outgoing-*")
		public ChannelInterceptor fooInterceptor() {
			return new ChannelInterceptor() {
				@Override
				public Message<?> preSend(Message<?> message, MessageChannel channel) {
					return MessageBuilder.fromMessage(message).setHeader("intercepted", "true").build();
				}
			};
		}

		@Bean
		public MessageHandler sendMessage(StreamBridge streamBridge) {
			return message -> {
				streamBridge.send("outgoing-out-0", message);
			};
		}
	}

}
