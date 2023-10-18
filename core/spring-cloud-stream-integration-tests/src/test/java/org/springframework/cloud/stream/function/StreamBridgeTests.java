/*
 * Copyright 2020-2023 the original author or authors.
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
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.cloudevent.CloudEventMessageBuilder;
import org.springframework.cloud.function.cloudevent.CloudEventMessageUtils;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.NewDestinationBindingCallback;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 */
public class StreamBridgeTests {

	@BeforeAll
	public static void before() {
		System.clearProperty("spring.cloud.function.definition");
	}

	@Test // see SCF-985
	void ensurePassThruFunctionIsNotPrePostProcessed() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<String> message = CloudEventMessageBuilder.withData("foo") // cloud event
					.setType("CustomType").setSource("CustomSource").setSubject("CustomSubject").build();
			streamBridge.send("fooDestination", message);

			Message<byte[]> messageReceived = outputDestination.receive(1000, "fooDestination");
			assertThat(CloudEventMessageUtils.getSource(messageReceived)).isEqualTo(URI.create("CustomSource"));
			assertThat(CloudEventMessageUtils.getSubject(messageReceived)).isEqualTo("CustomSubject");
			assertThat(CloudEventMessageUtils.getType(messageReceived)).isEqualTo("CustomType");
		}
	}

	@Test //
	void validateWARNifSendingToINputBinding() { // GH-2563
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(SimpleConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
						"--spring.cloud.function.definition=echo",
						"--spring.cloud.stream.function.bindings.echo-in-0=input")) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			streamBridge.send("input", "foo");
			// just validate that there is a WARN message about sending to the input binding
		}
	}

	@Test
	void extractStreamBridgeAsStreamOperations() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {
			StreamOperations streamOperations = context.getBean(StreamOperations.class);
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			assertThat(streamOperations).isSameAs(streamBridge);
		}
	}

	@Test
	void test_SCF_856() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			streamBridge.send("myBinding-out-0",
				CloudEventMessageBuilder.withData("hello").setSource("my-source")
					.setId(UUID.randomUUID().toString()).setSpecVersion("1.0").setType("myType")
					.setHeader(MessageUtils.TARGET_PROTOCOL, "kafka").build(),
				MimeTypeUtils.APPLICATION_JSON);
			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive();
			assertThat(result.getHeaders().get("ce_type")).isNotNull();
			assertThat(result.getHeaders().get("ce_source")).isNotNull();
		}
	}

	/*
	 * This test must not result in exception stating "Partition key cannot be null"
	 * See https://github.com/spring-cloud/spring-cloud-stream/issues/2249 for more details
	 */
	@Test
	void test_2249() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				EmptyConfiguration.class)).web(WebApplicationType.NONE).run(
			"--spring.cloud.stream.source=outputA;outputB",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-count=3",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-key-expression=headers['partitionKey']",
			"--spring.cloud.stream.bindings.outputB-out-0.destination=outputB",
			"--spring.cloud.stream.bindings.outputB-out-0.producer.partition-count=3",
			"--spring.jmx.enabled=false")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			streamBridge.send("outputA-out-0", MessageBuilder.withPayload("A").setHeader("partitionKey", "A").build());
			streamBridge.send("outputB", MessageBuilder.withPayload("B").build());
			streamBridge.send("outputA-out-0", MessageBuilder.withPayload("C").setHeader("partitionKey", "C").build());
			streamBridge.send("outputB", MessageBuilder.withPayload("D").build());

			OutputDestination output = context.getBean(OutputDestination.class);
			assertThat(output.receive(1000, "outputA-out-0").getHeaders().containsKey("scst_partition")).isTrue();
			assertThat(output.receive(1000, "outputB").getHeaders().containsKey("scst_partition")).isFalse();
			assertThat(output.receive(1000, "outputA-out-0").getHeaders().containsKey("scst_partition")).isTrue();
			assertThat(output.receive(1000, "outputB").getHeaders().containsKey("scst_partition")).isFalse();
		}
	}

	@Test
	void ensurePartitioningWorksWhenNativeEncodingEnabled() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				EmptyConfiguration.class)).web(WebApplicationType.NONE).run(
			"--spring.cloud.stream.source=outputA;outputB",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-count=3",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.use-native-encoding=true",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-key-expression=headers['partitionKey']")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			streamBridge.send("outputA-out-0", MessageBuilder.withPayload("A").setHeader("partitionKey", "A").build());
			streamBridge.send("outputA-out-0", MessageBuilder.withPayload("C").setHeader("partitionKey", "C").build());

			OutputDestination output = context.getBean(OutputDestination.class);
			assertThat(output.receive(1000, "outputA-out-0").getHeaders().containsKey("scst_partition")).isTrue();
			assertThat(output.receive(1000, "outputA-out-0").getHeaders().containsKey("scst_partition")).isTrue();
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	void test_2785() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				EmptyConfiguration.class)).web(WebApplicationType.NONE).run(
			"--spring.cloud.stream.source=outputA;outputB",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.use-native-encoding=false",
			"--spring.cloud.stream.bindings.outputB-out-0.producer.partition-count=1",
			"--spring.cloud.stream.bindings.outputC-out-0.producer.use-native-encoding=true",
			"--spring.cloud.stream.bindings.outputD-out-0.content-type=text/html",
			"--spring.jmx.enabled=false")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			Field field =  ReflectionUtils.findField(StreamBridge.class, "streamBridgeFunctionCache");
			Objects.requireNonNull(field).setAccessible(true);
			Map functionCache = (Map) field.get(streamBridge);

			streamBridge.send("outputA-out-0", MessageBuilder.withPayload("A").build());
			assertThat(functionCache.size()).isEqualTo(1);
			streamBridge.send("foo", MessageBuilder.withPayload("A").build());
			assertThat(functionCache.size()).isEqualTo(1);
			streamBridge.send("outputB-out-0", MessageBuilder.withPayload("A").build());
			assertThat(functionCache.size()).isEqualTo(1);
			streamBridge.send("outputC-out-0", MessageBuilder.withPayload("A").build());
			assertThat(functionCache.size()).isEqualTo(2);
			streamBridge.send("outputD-out-0", MessageBuilder.withPayload("A").build());
			assertThat(functionCache.size()).isEqualTo(3);
		}
	}

	/*
	 * This test verifies that when a partition key expression is set, then scst_partition is always set, even in
	 * concurrent scenarios.
	 * See https://github.com/spring-cloud/spring-cloud-stream/issues/2299 for more details
	 */
	@Test
	void test_2299_scstPartitionAlwaysSetEvenInConcurrentScenarios() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.source=outputA",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-count=3",
			"--spring.cloud.stream.bindings.outputA-out-0.producer.partition-key-expression=headers['partitionKey']",
			"--spring.jmx.enabled=false")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);

			int threadCount = 10;
			Set<Thread> threads = IntStream.range(0, threadCount)
				.mapToObj(i -> (Runnable) () -> IntStream.range(0, 100).forEach(j -> {
					String value = "M-" + i + "-" + j;
					streamBridge.send("outputA-out-0",
						MessageBuilder.withPayload(value).setHeader("partitionKey", value).build());
				})).map(Thread::new).collect(Collectors.toSet());

			threads.forEach(Thread::start);
			for (Thread thread : threads) {
				thread.join();
			}

			int messagesWithoutScstPartition = 0;
			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> message = output.receive(1000, "outputA-out-0");
			while (message != null) {
				if (!message.getHeaders().containsKey("scst_partition")) {
					messagesWithoutScstPartition++;
				}
				message = output.receive(1000, "outputA-out-0");
			}
			assertThat(messagesWithoutScstPartition).isEqualTo(0);
		}
	}

	@Test
	void testWithOutputContentTypeWildCardBindings() {
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

	// See this issue for more details: https://github.com/spring-cloud/spring-cloud-stream/issues/2805
	@Test
	void testStreamBridgeSendWithBinderNameAndCustomContentType() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(ConsumerConfiguration.class, EmptyConfigurationWithCustomConverters.class))
			.web(WebApplicationType.NONE).run(
				"--spring.cloud.stream.bindings.foo.content-type=application/*+foo")) {
			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo", "test-binder", "hello foo");

			OutputDestination output = context.getBean(OutputDestination.class);

			assertThat(output.receive(1000, "foo").getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeType.valueOf("application/json+foo"));
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	void testNoCachingOfStreamBridgeFunction() throws Exception {
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
			Objects.requireNonNull(field).setAccessible(true);
			Map<String, FunctionInvocationWrapper> map = (Map<String, FunctionInvocationWrapper>) field.get(bridge);
			assertThat(map.size()).isEqualTo(3);
		}
	}

	@Test
	void testDelayedSend() {
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
	void testWithInterceptorsMatchedAgainstAllPatterns() {
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

	@Test // validate that there is no exception thrown when sending to null channel
	void test_2268() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(InterceptorConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.jmx.enabled=false")) {
			StreamBridge bridge = context.getBean(StreamBridge.class);

			bridge.send("nullChannel", "blah");
		}
	}

	@Test
	void testInterceptorIsNotAddedMultipleTimesToTheMessageChannel() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(InterceptorConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.dynamic-destination-cache-size=1",
				"--spring.cloud.stream.output-bindings=outputA;outputB"
			)) {
			StreamBridge bridge = context.getBean(StreamBridge.class);

			bridge.send("outputA", "hello foo");
			bridge.send("outputA", "hello foo");
			bridge.send("outputA", "hello foo");
			bridge.send("outputA", "hello foo");

			AbstractMessageChannel messageChannel = context.getBean("outputA", AbstractMessageChannel.class);

			assertThat(messageChannel.getInterceptors()).hasSize(1);
		}
	}

	@Test
	void testBindingsAreRemovedWithCache() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(InterceptorConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.dynamic-destination-cache-size=1"
			)) {
			StreamBridge bridge = context.getBean(StreamBridge.class);

			bridge.send("a", "hello foo");
			bridge.send("b", "hello foo");
			bridge.send("c", "hello foo");
			bridge.send("d", "hello foo");

			BindingService bindingService = context.getBean(BindingService.class);
			assertThat(bindingService.getProducerBindingNames().length).isEqualTo(1);
			assertThat(bindingService.getProducerBindingNames()[0]).isEqualTo("d");
		}
	}

	@Test
	void testWithInterceptorsRegisteredOnlyOnOutputChannel() throws InterruptedException {
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
	void testBindingPropertiesAreHonored() {
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
	void testBridgeActivationWhenFunctionDefinitionIsPresent() {
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

	@Test
	void testNoBridgeIfNoSourcePropertyDefined() {
		assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
			.isThrownBy(() -> {
				ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
					.getCompleteConfiguration())
					.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
				context.getBean(StreamBridge.class);
			});
	}

	@Test
	void testBridgeFunctions() {
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
	void testBridgeFunctionsSendingMessagePreservingHeaders() {
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
	void testBridgeFunctionsWitthPartitionInformation() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=foo;bar",
				"--spring.cloud.stream.bindings.foo.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.foo.producer.partitionCount=5",
				"--spring.cloud.stream.bindings.bar.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.bar.producer.partitionCount=1",
				"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo", "a");
			bridge.send("bar", "b");
			bridge.send("foo", "c");
			bridge.send("foo", "d");
			bridge.send("bar", "e");
			bridge.send("foo", "f");
			bridge.send("bar", "g");


			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "foo");

			assertThat(new String(message.getPayload())).isEqualTo("a");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(2);

			message = outputDestination.receive(100, "foo");
			assertThat(new String(message.getPayload())).isEqualTo("c");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(4);

			message = outputDestination.receive(100, "foo");
			assertThat(new String(message.getPayload())).isEqualTo("d");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar");
			assertThat(new String(message.getPayload())).isEqualTo("b");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar");
			assertThat(new String(message.getPayload())).isEqualTo("e");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "bar");
			assertThat(new String(message.getPayload())).isEqualTo("g");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(0);

			message = outputDestination.receive(100, "foo");
			assertThat(new String(message.getPayload())).isEqualTo("f");
			assertThat(message.getHeaders().get("scst_partition")).isEqualTo(2);

			//assertThat(new String(outputDestination.receive(100, "bar-out-0").getPayload())).isEqualTo("b");
		}
	}

	@Test
	void testSendingMessageToOutputOfExistingSupplier() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(TestConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.stream.output-bindings=supplier;foo")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("foo-out-0", "b");

			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> message = outputDestination.receive(100, "foo-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("b");
			message = outputDestination.receive(100, "supplier-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("hello");
			bridge.send("supplier-out-0", "blah");
			message = outputDestination.receive(100, "supplier-out-0");
			assertThat(new String(message.getPayload())).isEqualTo("blah");
		}
	}

	@Test
	void testDynamicDestination() {
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
	void testWithIntegrationFlowBecauseMarcinSaidSo() {
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
	void testNewBindingCallback() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(BindingCallbackConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.stream.source=uppercase",
				"--spring.jmx.enabled=false")) {

			StreamBridge bridge = context.getBean(StreamBridge.class);
			bridge.send("uppercase-in-0", "hello");
			assertThat(context.getBean("callbackVerifier", AtomicBoolean.class)).isTrue();
		}
	}

	@Test
	void testDynamicProducerDestination() {
		System.clearProperty("spring.cloud.function.definition");
		ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(DynamicProducerDestinationConfig.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase",
				"--spring.cloud.stream.bindings.uppercase-in-0.destination=upper"
			);

		InputDestination source = context.getBean(InputDestination.class);
		source.send(new GenericMessage<>("John Doe".getBytes()), "upper");

		OutputDestination target = context.getBean(OutputDestination.class);
		Message<byte[]> message = target.receive(5, "dynamicTopic");

		assertThat(message).isNotNull();
		assertThat(new String(message.getPayload())).isEqualTo("JOHN DOE");
	}

	@EnableAutoConfiguration
	public static class DynamicProducerDestinationConfig {
		@Bean
		public Function<Message<String>, Message<String>> uppercase() {
			return msg -> MessageBuilder.withPayload(msg.getPayload().toUpperCase())
				.setHeader("spring.cloud.stream.sendto.destination", "dynamicTopic").build();
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
				protected boolean supports(@Nullable Class<?> clazz) {
					return true;
				}

				@Override
				protected Object convertFromInternal(@NonNull Message<?> message, @Nullable Class<?> targetClass, @Nullable Object conversionHint) {
					return message.getPayload();
				}

				@Override
				@Nullable
				protected Object convertToInternal(@NonNull Object payload, @NonNull MessageHeaders headers, @Nullable Object conversionHint) {
					if (headers.containsKey(MessageHeaders.CONTENT_TYPE) &&
						(Objects.requireNonNull(headers.get(MessageHeaders.CONTENT_TYPE)).toString().endsWith("+foo") ||
							Objects.requireNonNull(headers.get(MessageHeaders.CONTENT_TYPE)).toString().endsWith("+blah"))) {
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
				protected boolean supports(@NonNull Class<?> clazz) {
					return true;
				}

				@Override
				protected Object convertFromInternal(@NonNull Message<?> message, @Nullable Class<?> targetClass, @Nullable Object conversionHint) {
					return message.getPayload();
				}

				@Override
				@Nullable
				protected Object convertToInternal(@NonNull Object payload, @NonNull MessageHeaders headers, @Nullable Object conversionHint) {
					if (headers.containsKey(MessageHeaders.CONTENT_TYPE) &&
						(Objects.requireNonNull(headers.get(MessageHeaders.CONTENT_TYPE)).toString().endsWith("+non-registered-foo"))) {
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
				bridge.send("foo", v);
			};
		}
		@Bean
		public Function<String, Message<String>> function(StreamBridge bridge, BindingServiceProperties properties) {
			return v -> {
				int concurrency = properties.getConsumerProperties("foo").getConcurrency();
				int partitionCount = properties.getProducerProperties("foo").getPartitionCount();
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
				public Message<?> preSend(@NonNull Message<?> message, @Nullable MessageChannel channel) {
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
			return String::toUpperCase;
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
			return String::toUpperCase;
		}

		@Bean
		public AtomicBoolean callbackVerifier() {
			return new AtomicBoolean();
		}

		@Bean
		public NewDestinationBindingCallback<?> callback(AtomicBoolean callbackVerifier) {

			return (name, channel, props, extended) -> {
				callbackVerifier.set(true);
			};
		}
	}

	@EnableAutoConfiguration
	public static class IntegrationFlowConfiguration {

		@Bean
		public IntegrationFlow transform(StreamBridge bridge) {
			return IntegrationFlow.from("foo").transform(v -> {
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
			return IntegrationFlow.from(inputChannel)
				.log(LoggingHandler.Level.INFO, (m) -> {
					LATCH1.countDown();
					return "Going through the first flow: " + m.getPayload();
				})
				.handle(sendMessage)
				.get();
		}

		@Bean
		public IntegrationFlow someOtherFlow(MessageHandler sendMessage) {
			return IntegrationFlow.from(otherInputChannel())
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
				public Message<?> preSend(@NonNull Message<?> message, @Nullable MessageChannel channel) {
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
