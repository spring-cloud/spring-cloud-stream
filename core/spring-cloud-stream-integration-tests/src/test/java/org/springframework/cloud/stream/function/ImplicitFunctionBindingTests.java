/*
 * Copyright 2019-2023 the original author or authors.
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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.catalog.FunctionAroundWrapper;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.test.FunctionBindingTestUtils;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.cloud.stream.binding.DefaultPartitioningInterceptor;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ResolvableType;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.support.PeriodicTrigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 *
 */
class ImplicitFunctionBindingTests {

	@AfterEach
	public void after() {
		System.clearProperty("spring.cloud.function.definition");
	}


	@Test
	void testFailedApplicationListenerConfiguration() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FailedApplicationListenerConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=echo")) {

			InputDestination input = context.getBean(InputDestination.class);
			input.send(new GenericMessage<byte[]>("hello".getBytes()), "echo-in-0");

			OutputDestination output = context.getBean(OutputDestination.class);
			Message<byte[]> result = output.receive(1000, "echo-out-0");
			assertThat(result.getPayload()).isEqualTo("hello".getBytes());
		}
	}

	@SuppressWarnings({"rawtypes"})
	@Test
	void testDisableAutodetect() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SendToDestinationConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.stream.function.autodetect=false")) {

			BindingsLifecycleController ctrl = context.getBean(BindingsLifecycleController.class);
			var input = ctrl.queryState("echo-in-0");
			var output = ctrl.queryState("echo-out-0");
			assertThat(input).isEmpty();
			assertThat(output).isEmpty();
		}
	}


	@SuppressWarnings({"rawtypes" })
	@Test
	void testBindingControl() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SendToDestinationConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false")) {

			BindingsLifecycleController ctrl = context.getBean(BindingsLifecycleController.class);
			Binding input = ctrl.queryState("echo-in-0").get(0);
			assertThat(input.isRunning()).isTrue();
			ctrl.changeState("echo-in-0", State.STOPPED);
			assertThat(input.isRunning()).isFalse();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"single-destination", "destination1,destination2,destination3"})
	void testGh2658_WithMultipleDestinations(String destination) {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleConsumerWithMultipleDestinationConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=consumerMultiple",
				"--spring.cloud.stream.bindings.consumerMultiple-in-0.group=group",
				"--spring.cloud.stream.bindings.consumerMultiple-in-0.destination=" + destination)) {

			BindingsLifecycleController ctrl = context.getBean(BindingsLifecycleController.class);
			var multipleInput = ctrl.queryState("consumerMultiple-in-0");

			assertThat(multipleInput).hasSize(destination.split(",").length);
			multipleInput.stream().forEach(binding -> assertThat(binding.isRunning()).isTrue());

			ctrl.changeState("consumerMultiple-in-0", State.STOPPED);
			multipleInput.stream().forEach(binding -> assertThat(binding.isRunning()).isFalse());

			ctrl.changeState("consumerMultiple-in-0", State.STARTED);
			multipleInput.stream().forEach(binding -> assertThat(binding.isRunning()).isTrue());
		}
	}

	@Test
	void testGh2658_queryBindingThatNotExists() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleConsumerWithMultipleDestinationConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=consumerMultiple",
				"--spring.cloud.stream.bindings.consumerMultiple-in-0.group=group",
				"--spring.cloud.stream.bindings.consumerMultiple-in-0.destination=destination")) {

			BindingsLifecycleController ctrl = context.getBean(BindingsLifecycleController.class);
			var inputBindingList = ctrl.queryState("bindingNotExist-in-0");
			assertThat(inputBindingList).isEmpty();
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	void dynamicBindingTestWithFunctionRegistrationAndExplicitDestination() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.stream.bindings.function-in-0.destination=input")) {

			InputDestination input = context.getBean(InputDestination.class);
			try {
				input.send(new GenericMessage<byte[]>("hello".getBytes()));
				fail(); // it should since there are no functions and no bindings
			}
			catch (Exception e) {
				// good, we expected it
			}

			Function<byte[], byte[]> function = v -> v;
			FunctionRegistration functionRegistration = new FunctionRegistration(function, "function");
			functionRegistration = functionRegistration.type(ResolvableType.forClassWithGenerics(Function.class, Object.class, Object.class).getType());
			FunctionBindingTestUtils.bind(context, functionRegistration);

			input.send(new GenericMessage<byte[]>("hello".getBytes()), "input");

			OutputDestination output = context.getBean(OutputDestination.class);
			assertThat(output.receive(1000, "function-out-0").getPayload()).isEqualTo("hello".getBytes());
		}
	}

	@Test
	void dynamicBindingTestWithFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false")) {
			InputDestination input = context.getBean(InputDestination.class);
			try {
				input.send(new GenericMessage<byte[]>("hello".getBytes()));
				fail(); // it should since there are no functions and no bindings
			}
			catch (Exception e) {
				// good, we expected it
			}

			Function<byte[], String> function = v -> new String(v).toUpperCase();
			FunctionBindingTestUtils.bind(context, function);

			input.send(new GenericMessage<byte[]>("hello".getBytes()));

			OutputDestination output = context.getBean(OutputDestination.class);
			assertThat(new String(output.receive().getPayload())).isEqualTo("HELLO");
		}
	}

	@Test
	void testEmptyConfiguration() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(EmptyConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false", "--debug")) {
			context.getBean(InputDestination.class);
		}
		catch (Exception e) { // should not fail
			fail();
		}
	}

	@Test
	void testNullMessage() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NullMessagerConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=func;  funcMessage")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);


			Message<?> inputMessage = MessageBuilder.withPayload(KafkaNull.INSTANCE).build();
			inputDestination.send(inputMessage, "func-in-0");

			Message<byte[]> outputMessage = outputDestination.receive(1000, "func-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("NULL".getBytes());

			inputDestination.send(inputMessage, "funcMessage-in-0");

			outputMessage = outputDestination.receive(1000, "funcMessage-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("NULL".getBytes());
		}
	}

	@Test
	void testSimpleFunctionWithStreamProperty() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoEnableBindingConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	void testHeaderRetentionWithComposition() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoEnableBindingConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.autodetect=false",
				"--spring.cloud.function.definition=func|addHeaders")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	void testReactiveFunctionWithState() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoEnableBindingConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=aggregate")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);
			inputDestination.send(inputMessage);
			inputDestination.send(inputMessage);
			assertThat(new String(outputDestination.receive(2000, "aggregate-out-0").getPayload())).isEqualTo("HelloHelloHello");
			assertThat(new String(outputDestination.receive(2000, "aggregate-out-0").getPayload())).isEqualTo("");

			inputDestination.send(inputMessage);
			inputDestination.send(inputMessage);
			inputDestination.send(inputMessage);
			inputDestination.send(inputMessage);
			assertThat(new String(outputDestination.receive(2000, "aggregate-out-0").getPayload())).isEqualTo("HelloHelloHelloHello");
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testFunctionWithUseNativeEncoding() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoEnableBindingConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=func",
				"--spring.cloud.stream.bindings.func-out-0.producer.useNativeEncoding=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);
			Message outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello");
		}
	}

	@Test
	void testSimpleFunctionWithNativeProperty() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoEnableBindingConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	void testSimpleFunctionWithoutDefinitionProperty() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	void testSimpleConsumerWithoutDefinitionProperty() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleConsumerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("consumer")).isEqualTo("Hello");
			System.clearProperty("consumer");
		}
	}

	@Test
	void fooFunctionComposedWithConsumerNoOutputChannel() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveFunctionConfiguration.class, SingleReactiveConsumerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=echo|consumer")) {

			assertThat(context.containsBean("echoconsumer-out-0")).isFalse();

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("consumer")).isEqualTo("Hello");
			System.clearProperty("consumer");
		}
	}

	@Test
	void fooFunctionComposedWithConsumerNonReactive() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionConsumerCopositionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false", "--spring.cloud.function.definition=echo|consumer")) {

			assertThat(context.containsBean("echoconsumer-out-0")).isFalse();

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("FunctionConsumerCopositionConfiguration")).isEqualTo("Hello");
			System.clearProperty("FunctionConsumerCopositionConfiguration");
		}
	}

	@Test
	void testReactiveConsumerWithoutDefinitionProperty() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleReactiveConsumerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("consumer")).isEqualTo("Hello");
			System.clearProperty("consumer");
		}
	}

	@Test
	void testConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleConsumerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=consumer",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.consumer-in-0.content-type=text/plain")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("John Doe".getBytes()));
		}
	}

	@Test
	void testBindingWithReactiveFunction() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=echo")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder.withPayload("Hello".getBytes()).build();
			Message<byte[]> inputMessageTwo = MessageBuilder.withPayload("Hello Again".getBytes()).build();
			inputDestination.send(inputMessageOne);
			inputDestination.send(inputMessageTwo);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
			outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello Again".getBytes());
		}
	}

//	@Test
//	public void testFunctionConfigDisabledIfStreamListenerIsUsed() {
//		System.clearProperty("spring.cloud.function.definition");
//		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
//				TestChannelBinderConfiguration.getCompleteConfiguration(LegacyConfiguration.class))
//						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {
//
//			assertThat(context.getBean("supplierInitializer").getClass().getSimpleName()).isEqualTo("NullBean");
//		}
//	}

	@Test
	void testDeclaredTypeVsActualInstance() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SCF_GH_409Configuration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder.withPayload("Hello".getBytes()).build();

			inputDestination.send(inputMessageOne);
			fail();
		}
		catch (Exception ex) {
			// good
		}
	}

	@Test
	void testWithContextTypeApplicationProperty() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.content-type=text/plain", "--debug")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder.withPayload("Hello".getBytes()).build();
			Message<byte[]> inputMessageTwo = MessageBuilder.withPayload("Hello Again".getBytes()).build();
			inputDestination.send(inputMessageOne);
			inputDestination.send(inputMessageTwo);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
			outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello Again".getBytes());
		}
	}

	@Test
	void testWithIntegrationFlowAsFunction() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(FunctionSampleSpringIntegrationConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("hello".getBytes()).build();

			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
		}
	}

	@Test
	@Disabled
	void testSupplierWithCustomPoller() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SupplierWithExplicitPollerConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false", "--spring.cloud.stream.poller.fixed-delay=1500")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(((PeriodicTrigger) pollerMetadata.getTrigger()).getPeriodDuration()).isEqualTo(Duration.ofMillis(1500));

			Message<byte[]> outputMessage = outputDestination.receive(6000);
			assertThat(outputMessage.getPayload()).isEqualTo("hello".getBytes());
		}
	}

	@Test
	@Disabled
	void testImperativeSupplierReactiveFunctionComposition() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ImperativeSupplierComposedWithReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.poller.fixed-delay=1000",
				"--spring.cloud.function.definition=supplier|functionA")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> outputMessage = outputDestination.receive(2000, "supplierfunctionA-out-0");
			Long value = Long.parseLong(new String(outputMessage.getPayload()));

			outputMessage = outputDestination.receive(5000, "supplierfunctionA-out-0");
			assertThat(Long.parseLong(new String(outputMessage.getPayload())) - value).isGreaterThanOrEqualTo(1000);

			outputMessage = outputDestination.receive(5000, "supplierfunctionA-out-0");
			assertThat(Long.parseLong(new String(outputMessage.getPayload())) - value).isGreaterThanOrEqualTo(1000);

			outputMessage = outputDestination.receive(5000, "supplierfunctionA-out-0");
			assertThat(Long.parseLong(new String(outputMessage.getPayload())) - value).isGreaterThanOrEqualTo(1000);
		}
	}



	@Test
	@Disabled
	void testSupplierWithCustomPollerAndMappedOutput() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SupplierWithExplicitPollerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.stream.poller.fixed-delay=1200",
				"--spring.cloud.function.bindings.supplier-out-0=output")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			PollerMetadata pollerMetadata = context.getBean(PollerMetadata.class);
			assertThat(((PeriodicTrigger) pollerMetadata.getTrigger()).getPeriodDuration()).isEqualTo(Duration.ofMillis(1200));

			Message<byte[]> outputMessage = outputDestination.receive(10000);
			assertThat(outputMessage.getPayload()).isEqualTo("hello".getBytes());
		}
	}

	@Test
	void testNoFunctionEnabledConfiguration() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(NoFunctionEnabledConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			try {
				context.getBean(FunctionConfiguration.class);
				fail();
			}
			catch (Exception e) {
				// ignore
			}

		}
	}

	@Test
	void functionInvocationWrapperReflectsBiConsumerTargetFunctionType() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(WrappedBiConsumerAutoConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {
			FunctionCatalog functionCatalog = context.getBean(FunctionCatalog.class);
			FunctionInvocationWrapper functionWrapper = functionCatalog.lookup("testBiConsumer");
			assertThat(functionWrapper.isWrappedBiConsumer()).isTrue();
		}
	}

	@Test
	void testCollectionAndMapConversionDuringComposition() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(CompositionWithCollectionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=funcA|funcB",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("[{\"key1\":1, \"key2\":2},{\"key3\":3}]".getBytes()).build();

			inputDestination.send(inputMessage);
			String result = new String(outputDestination.receive().getPayload());

			assertThat(result).isEqualTo("[{\"key1\":\"1\",\"key2\":\"2\"},{\"key3\":\"3\"}]");
		}
	}

	// see https://github.com/spring-cloud/spring-cloud-stream/issues/1896
	@Test
	void testOutputAsCollectionOfMessages() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(SplittableTypesConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=funcCollectionOfMessages",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa,bb,cc,dd".getBytes()).build();

			inputDestination.send(inputMessage);

			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("aa");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("bb");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("cc");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("dd");
			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void testOutputAsArrayOfMessages() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(SplittableTypesConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=funcArrayOfMessages",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa,bb,cc,dd".getBytes()).build();

			inputDestination.send(inputMessage);

			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("aa");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("bb");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("cc");
			assertThat(new String(outputDestination.receive(100).getPayload())).isEqualTo("dd");
			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void testWithExplicitBindingInstructions() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(SplittableTypesConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=funcArrayOfMessages",
				"--spring.cloud.stream.function.bindings.funcArrayOfMessages-in-0=input",
				"--spring.cloud.stream.function.bindings.funcArrayOfMessages-out-0=output",
				"--spring.cloud.stream.bindings.input.destination=myInput",
				"--spring.cloud.stream.bindings.output.destination=myOutput",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa,bb,cc,dd".getBytes()).build();

			inputDestination.send(inputMessage, "myInput");

			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("aa");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("bb");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("cc");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("dd");
			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void testWithExplicitBindingInstructionsOnlyDestination() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(SplittableTypesConfiguration.class))
			.web(WebApplicationType.NONE).run(
				"--spring.cloud.function.definition=funcArrayOfMessages",
				"--spring.cloud.stream.bindings.funcArrayOfMessages-in-0.destination=myInput",
				"--spring.cloud.stream.bindings.funcArrayOfMessages-out-0.destination=myOutput",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa,bb,cc,dd".getBytes()).build();

			inputDestination.send(inputMessage, "myInput");

			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("aa");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("bb");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("cc");
			assertThat(new String(outputDestination.receive(100, "myOutput").getPayload())).isEqualTo("dd");
			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void partitionOnOutputPayloadTest() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=func",
				"--spring.cloud.stream.bindings.func-out-0.producer.partitionKeyExpression=payload.id",
				"--spring.cloud.stream.bindings.func-out-0.producer.partitionCount=5",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Jim Lahey".getBytes()).build();

			inputDestination.send(inputMessage, "func-in-0");

			assertThat(outputDestination.receive(100, "func-out-0").getHeaders().get("scst_partition")).isEqualTo(3);

			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void partitionOnOutputPayloadAsListAndFunctionAroundWrapperTest() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=uppercase|persons",
				"--spring.cloud.stream.bindings.uppercase|persons-out-0.producer.partitionKeyExpression=payload.id",
				"--spring.cloud.stream.bindings.uppercase|persons-out-0.producer.partitionCount=5",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Jim Lahey".getBytes()).build();

			inputDestination.send(inputMessage, "uppercasepersons-in-0");

			assertThat(outputDestination.receive(100, "uppercasepersons-out-0").getHeaders().get("scst_partition")).isEqualTo(3);

			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void partitionOnOutputPayloadTestReactive() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=funcReactive",
				"--spring.cloud.stream.bindings.funcReactive-out-0.producer.partitionKeyExpression=payload.id",
				"--spring.cloud.stream.bindings.funcReactive-out-0.producer.partitionCount=5",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Jim Lahey".getBytes()).build();

			inputDestination.send(inputMessage, "funcReactive-in-0");

			assertThat(outputDestination.receive(100, "funcReactive-out-0").getHeaders().get("scst_partition")).isEqualTo(3);

			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void partitionOnOutputPayloadWithSupplierTest() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=personSupplier",
				"--spring.cloud.stream.bindings.personSupplier-out-0.producer.partitionKeyExpression=payload.id",
				"--spring.cloud.stream.bindings.personSupplier-out-0.producer.partitionCount=5",
				"--spring.jmx.enabled=false")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			assertThat(outputDestination.receive(1000, "personSupplier-out-0").getHeaders().get("scst_partition")).isEqualTo(1);

			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void partitionOnOutputPayloadWithReactiveSupplierTest() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=reactivePersonSupplier",
				"--spring.cloud.stream.bindings.reactivePersonSupplier-out-0.producer.partitionKeyExpression=payload.id",
				"--spring.cloud.stream.bindings.reactivePersonSupplier-out-0.producer.partitionCount=5",
				"--spring.jmx.enabled=false")) {

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			assertThat(outputDestination.receive(1000, "reactivePersonSupplier-out-0").getHeaders().get("scst_partition")).isEqualTo(1);

			assertThat(outputDestination.receive(100)).isNull();
		}
	}

	@Test
	void contentTypeAsByteArrayTest() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestChannelBinderConfiguration
			.getCompleteConfiguration(PojoFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.cloud.function.definition=echoPerson",
				"--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("{\"name\":\"Jim Lahey\",\"id\":420}".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/json".getBytes(StandardCharsets.UTF_8))
				.build();

			inputDestination.send(inputMessage, "echoPerson-in-0");

			assertThat(outputDestination.receive(100, "echoPerson-out-0").getPayload()).isEqualTo("{\"name\":\"Jim Lahey\",\"id\":420}".getBytes());
		}
	}

	@Test
	void testReactiveFunctionWithOutputAsMonoVoid() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionalConsumerConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			assertThat(context.containsBean("funcConsumer-out-0")).isFalse();
			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("consumer")).isEqualTo("Hello");
			System.clearProperty("consumer");
		}
	}

	@Test
	void testReactiveSendToDestinationConfiguration() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SendToDestinationConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa".getBytes()).build();
			inputDestination.send(inputMessage, "echo-in-0");
			Message<byte[]> receivedMessage = outputDestination.receive(1000, "aa");

			assertThat(receivedMessage.getPayload()).isEqualTo("aa".getBytes());
			assertThat(receivedMessage.getHeaders().get("spring.cloud.stream.sendto.destination")).isNotNull();

			inputMessage = MessageBuilder.withPayload("bb".getBytes()).build();
			inputDestination.send(inputMessage, "echo-in-0");
			receivedMessage = outputDestination.receive(1000, "bb");

			assertThat(receivedMessage.getPayload()).isEqualTo("bb".getBytes());
			assertThat(receivedMessage.getHeaders().get("spring.cloud.stream.sendto.destination")).isNotNull();
		}
	}

	// See this issue for more context on this test: https://github.com/spring-cloud/spring-cloud-stream/issues/2770
	@Test
	void testSendToDestinationWhenPartitionsEnabled() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ImperativeSendToDestinationConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				// partition-key-extractor also works below, but for easy testing we picked partition-key-expression since
				// partition-key-extractor requires defining a bean.
				"--spring.cloud.stream.bindings.aa.producer.partition-key-expression=payload",
				"--spring.cloud.stream.bindings.bb.producer.partition-key-expression=payload")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder.withPayload("aa".getBytes()).build();
			inputDestination.send(inputMessage, "echo-in-0");
			Message<byte[]> receivedMessage = outputDestination.receive(1000, "aa");

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			MessageChannel aa = streamBridge.resolveDestination("aa", null, null);
			List<ChannelInterceptor> interceptors1 = ((AbstractMessageChannel) aa).getInterceptors();

			assertThat(interceptors1.size()).isEqualTo(1);
			assertThat(interceptors1.get(0)).isInstanceOf(DefaultPartitioningInterceptor.class);

			assertThat(receivedMessage.getPayload()).isEqualTo("aa".getBytes());
			assertThat(receivedMessage.getHeaders().get("spring.cloud.stream.sendto.destination")).isNotNull();

			inputMessage = MessageBuilder.withPayload("bb".getBytes()).build();
			inputDestination.send(inputMessage, "echo-in-0");
			receivedMessage = outputDestination.receive(1000, "bb");

			MessageChannel bb = streamBridge.resolveDestination("bb", null, null);
			List<ChannelInterceptor> interceptors2 = ((AbstractMessageChannel) bb).getInterceptors();

			assertThat(interceptors2.size()).isEqualTo(1);
			assertThat(interceptors2.get(0)).isInstanceOf(DefaultPartitioningInterceptor.class);

			assertThat(receivedMessage.getPayload()).isEqualTo("bb".getBytes());
			assertThat(receivedMessage.getHeaders().get("spring.cloud.stream.sendto.destination")).isNotNull();
		}
	}


	@Test
	@ExtendWith(OutputCaptureExtension.class)
	void testReactiveConsumerWithConcurrencyGreaterThanOneLogsWarning(CapturedOutput output) {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext ignored = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveConsumerWithConcurrencyFailureConfiguration.class))
				.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
					"--spring.cloud.stream.bindings.input-in-0.consumer.concurrency=2")) {
			assertThat(output).contains("When using concurrency > 1 in reactive contexts, please make sure that you are using a reactive binder");
		}
	}

	@Test
	void testGh1973() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SupplierAndProcessorConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=echo;supplier",
				"--spring.cloud.stream.bindings.supplier-out-0.destination=output",
				"--spring.cloud.stream.bindings.echo-out-0.destination=output")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			inputDestination.send(MessageBuilder.withPayload("hello").build());

			assertThat(outputDestination.receive(1000, "output")).isNotNull();
			assertThat(outputDestination.receive(1000, "output")).isNull();
			assertThat(outputDestination.receive(1000, "output")).isNull();

		}
	}

	@Test
	void testGh1944() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SupplierAndPojoConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=supplier")) {
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> result = outputDestination.receive(1000, "supplier-out-0");
			assertThat(new String(result.getPayload())).isEqualTo("[{\"name\":\"Ricky\",\"id\":1},{\"name\":\"Julien\",\"id\":2}]");
		}
	}

	@Test
	void testGh2054() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=reactivePojoMessage")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			String jsonPerson = "{\"name\":\"Ricky\",\"id\":1}";

			inputDestination.send(MessageBuilder.withPayload(jsonPerson.getBytes()).build());

			Message<byte[]> result = outputDestination.receive(2000);
			assertThat(new String(result.getPayload())).isEqualTo("{\"name\":\"RICKY\",\"id\":1}");
		}
	}


	@Test
	void testGh2062() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=echo")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			String jsonPerson = "error";

			inputDestination.send(MessageBuilder.withPayload(jsonPerson.getBytes()).build());
			// there is really nothing to assert other then check the logs for stack trace and error message
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testWithNativeEncodingImperative() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=imperative")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message result = outputDestination.receive(2000);
			assertThat(result.getPayload()).isInstanceOf(byte[].class); // check output type
			assertThat(new String((byte[]) result.getPayload())).isEqualTo("byte[]"); // check input type
		}

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=imperative",
				"--spring.cloud.stream.bindings.imperative-in-0.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.imperative-out-0.producer.useNativeEncoding=true"
			)) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message result = outputDestination.receive(2000);
			assertThat(result.getPayload()).isInstanceOf(String.class); // check output type
			assertThat(result.getPayload()).isEqualTo("byte[]"); // check input type
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testWithNativeEncodingReactive() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=reactive")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message result = outputDestination.receive(2000);
			assertThat(result.getPayload()).isInstanceOf(byte[].class); // check output type
			assertThat(new String((byte[]) result.getPayload())).isEqualTo("byte[]"); // check input type
		}
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=reactive",
				"--spring.cloud.stream.bindings.reactive-in-0.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.reactive-out-0.producer.useNativeEncoding=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message result = outputDestination.receive(2000);
			assertThat(result.getPayload()).isInstanceOf(String.class); // no output conversion to byte[] has happened.
			assertThat(result.getPayload()).isEqualTo("byte[]");
		}

		//Consumer reactiveConsumer
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=reactiveConsumer",
				"--spring.cloud.stream.bindings.reactiveConsumer-in-0.consumer.useNativeDecoding=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			QueueChannel testChannel = context.getBean("testChannel", QueueChannel.class);

			Message result = testChannel.receive(2000);
			assertThat(result.getPayload()).isEqualTo(byte[].class.getName());
		}

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration2.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=reactiveFunctionConsumer",
				"--spring.cloud.stream.bindings.reactiveFunctionConsumer-in-0.consumer.useNativeDecoding=true")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			inputDestination.send(new GenericMessage<byte[]>("hello".getBytes()));

			QueueChannel testChannel = context.getBean("testChannel", QueueChannel.class);

			Message result = testChannel.receive(2000);
			assertThat(result.getPayload()).isEqualTo(byte[].class.getName());
		}
	}

	@Test
	void testHeaderPropagation() {
		System.clearProperty("spring.cloud.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SingleFunctionConfiguration.class))
			.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			inputDestination.send(MessageBuilder.withPayload("hello").setHeader("foo", "bar").build());

			assertThat(outputDestination.receive(1000).getHeaders().get("foo")).isEqualTo("bar");
		}
	}


	@EnableAutoConfiguration
	public static class SupplierAndPojoConfiguration {
		@Bean
		public Supplier<List<Person>> supplier() {
			Person p1 = new Person();
			p1.setId(1);
			p1.setName("Ricky");
			Person p2 = new Person();
			p2.setId(2);
			p2.setName("Julien");
			return () -> Arrays.asList(p1, p2);
		}
	}

	@EnableAutoConfiguration
	public static class SupplierAndProcessorConfiguration {
		Many<Message<String>> sink = Sinks.many().unicast().onBackpressureBuffer();

		@Bean
		public Supplier<Flux<Message<String>>> supplier() {
			return () -> sink.asFlux().doOnNext(v -> {
				System.out.println("Hello " + v);
			});
		}

		@Bean
		public Function<Message<String>, Message<String>> echo() {
			return v -> v;
		}
	}

	@EnableAutoConfiguration
	public static class ReactiveConsumerWithConcurrencyFailureConfiguration {
		@Bean
		public Consumer<Flux<Message<String>>> input() {
			return flux -> flux.subscribe(System.out::println);
		}
	}

	@EnableAutoConfiguration
	public static class NoEnableBindingConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}

		@Bean
		public Function<Flux<String>, Flux<Message<String>>> addHeaders() {
			return flux -> flux.map(value -> {
				return MessageBuilder.withPayload(value).setHeader("foo", "bar").build();
			});
		}

		@Bean
		public Function<Flux<String>, Flux<String>> aggregate() {
			return inbound -> inbound.log().window(Duration.ofSeconds(1))
				.flatMap(w -> w.reduce("", (s1, s2) -> s1 + s2)).log();
		}

		@Bean
		public Consumer<String> cons() {
			return x -> {
				System.out.println("Consumer");
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleFunctionConfiguration2 {

		@Bean
		public QueueChannel testChannel() {
			return new QueueChannel();
		}

		@Bean
		public Function<Object, String> imperative() {
			return x -> {
				return x.getClass().getSimpleName();
			};
		}

		@Bean
		public Function<Flux<Object>, Flux<String>> reactive() {
			return flux -> flux.map(x -> {
				return x.getClass().getSimpleName();
			});
		}

		@Bean
		public Consumer<Flux<Message<?>>> reactiveConsumer(MessageChannel testChannel) {
			return flux -> flux.subscribe(v -> {
				testChannel.send(new GenericMessage<String>(((Message<?>) v).getPayload().getClass().getName()));
			});
		}

		@Bean
		public Function<Flux<Message<?>>, Mono<Void>> reactiveFunctionConsumer(MessageChannel testChannel) {
			return flux -> flux.doOnNext(x -> {
				testChannel.send(new GenericMessage<String>(((Message<?>) x).getPayload().getClass().getName()));
			}).then();
		}
	}

	@EnableAutoConfiguration
	public static class SingleFunctionConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleConsumerConfiguration {

		@Bean
		public Consumer<String> consumer() {
			return value -> {
				System.out.println(value);
				System.setProperty("consumer", value);
			};
		}
	}

	@EnableAutoConfiguration
	public static class NullMessagerConfiguration {

		@Bean
		public Function<Person, String> func() {
			return value -> {
				assertThat(value).isNull();
				return "NULL";
			};
		}

		@Bean
		public Function<Message<Person>, String> funcMessage() {
			return value -> {
				Object v = value.getPayload();
				String className = v.getClass().getName();
				assertThat(className).isEqualTo("org.springframework.kafka.support.KafkaNull");
				return "NULL";
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleReactiveConsumerConfiguration {

		@Bean
		public Consumer<Flux<String>> consumer() {
			return flux -> flux.subscribe(value -> {
				System.out.println(value);
				System.setProperty("consumer", value);
			});
		}
	}

	@EnableAutoConfiguration
	public static class FunctionConsumerCopositionConfiguration {

		@Bean
		public Consumer<String> consumer() {
			return v -> {
				System.out.println("==== Consuming " + v);
				System.setProperty("FunctionConsumerCopositionConfiguration", v);
			};
		}

		@Bean
		public Function<String, String> echo() {
			return v -> {
				System.out.println("==> Echo " + v);
				return v;
			};
		}
	}

	@EnableAutoConfiguration
	public static class ReactiveFunctionConfiguration {

		@Bean
		public Function<Flux<String>, Flux<String>> echo() {
			return flux -> flux.map(value -> {
				System.out.println("echo value reqctive " + value);
				if (value.equals("error")) {
					throw new RuntimeException("intentional");
				}
				return value;
			});
		}

		@Bean
		public Function<Flux<Message<Person>>, Flux<Message<Person>>> reactivePojoMessage() {
			return flux -> flux.map(message -> {
				Person p = message.getPayload();
				p.setName(p.getName().toUpperCase());
				return MessageBuilder.withPayload(p).copyHeaders(message.getHeaders()).build();
			});
		}
	}

	@EnableAutoConfiguration
	public static class FunctionalConsumerConfiguration {
		@Bean
		public Function<Flux<String>, Mono<Void>> funcConsumer() {
			return flux -> flux.doOnNext(value -> {
				System.out.println(value);
				System.setProperty("consumer", value);
			}).then();
		}
	}

	@EnableAutoConfiguration
	public static class EmptyConfiguration {

	}

	@EnableAutoConfiguration
	public static class SendToDestinationConfiguration {
		@Bean
		public Function<Flux<String>, Flux<Message<String>>> echo() {
			return flux -> flux.map(v -> {
				return MessageBuilder.withPayload(v).setHeader("spring.cloud.stream.sendto.destination", v).build();
			});
		}
	}

	@EnableAutoConfiguration
	public static class ImperativeSendToDestinationConfiguration {
		@Bean
		public Function<String, Message<String>> echo() {
			return s -> MessageBuilder.withPayload(s).setHeader("spring.cloud.stream.sendto.destination", s).build();
		}
	}

	@EnableAutoConfiguration
	public static class SCF_GH_409Configuration {

		@Bean
		public Serializable blah() {
			return new Foo();
		}

		@SuppressWarnings("serial")
		private static class Foo implements Supplier<Object>, Serializable {

			@Override
			public Object get() {
				// TODO Auto-generated method stub
				return null;
			}

		}
	}

	@EnableAutoConfiguration
	public static class FunctionSampleSpringIntegrationConfiguration {

		@Bean
		public IntegrationFlow uppercaseFlow() {
			return IntegrationFlow.from(MessageFunction.class, gateway -> gateway.beanName("uppercase"))
				.<String, String>transform(String::toUpperCase).get();
		}

	}

	public interface MessageFunction extends Function<Message<String>, Message<String>> {

	}

	@EnableAutoConfiguration
	public static class SupplierWithExplicitPollerConfiguration {

		@Bean
		public Supplier<String> supplier() {
			return () -> "hello";
		}
	}

	@EnableAutoConfiguration
	public static class ImperativeSupplierComposedWithReactiveFunctionConfiguration {

		@Bean
		public Supplier<Long> supplier() {
			return () -> System.currentTimeMillis();
		}

		@Bean
		public Function<Flux<String>, Flux<String>> functionA() {
			return flux -> flux;
		}

	}

	@EnableAutoConfiguration(exclude = ContextFunctionCatalogAutoConfiguration.class)
	public static class NoFunctionEnabledConfiguration {

		@Bean
		public Supplier<String> supplier() {
			return () -> "hello";
		}
	}

	@EnableAutoConfiguration
	public static class CompositionWithCollectionConfiguration {

		@Bean
		public Function<Message<List<Map<String, Integer>>>, Message<List<Map<String, Integer>>>> funcA() {
			return v -> v;
		}

		@Bean
		public Function<Message<List<Map<String, String>>>, Message<List<Map<String, String>>>> funcB() {
			return v -> {
				return v;
			};
		}
	}

	@EnableAutoConfiguration
	public static class SplittableTypesConfiguration {

		@Bean
		public Function<String, Collection<Message<String>>> funcCollectionOfMessages() {
			return v -> {
				String[] values = v.split(",");
				List<Message<String>> messages = new ArrayList<>();
				for (String value : values) {
					messages.add(MessageBuilder.withPayload(value).build());
				}
				return messages;
			};
		}

		@SuppressWarnings("unchecked")
		@Bean
		public Function<String, Message<String>[]> funcArrayOfMessages() {
			return v -> {
				String[] values = v.split(",");
				List<Message<String>> messages = new ArrayList<>();
				for (String value : values) {
					messages.add(MessageBuilder.withPayload(value).build());
				}
				return messages.toArray(new Message[0]);
			};
		}
	}

	@EnableAutoConfiguration
	public static class PojoFunctionConfiguration {

		@Bean
		public Function<Person, Person> echoPerson() {
			return x -> x;
		}

		@Bean
		public FunctionAroundWrapper faw() {
			return new FunctionAroundWrapper() {

				@Override
				protected Object doApply(Object input, FunctionInvocationWrapper targetFunction) {
					return targetFunction.apply(input);
				}
			};
		}

		@Bean
		public Supplier<Person> personSupplier() {
			Person p = new Person();
			p.setId(21);
			p.setName("Jim Lehey");
			return () -> p;
		}

		@Bean
		public Supplier<Flux<Person>> reactivePersonSupplier() {
			return () ->  {
				Person p = new Person();
				p.setId(21);
				p.setName("Jim Lehey");
				return Flux.just(p);
			};
		}

		@Bean
		public Function<String, Person> func() {
			return x -> {
				Person person = new Person();
				person.setName(x);
				person.setId(3);
				return person;
			};
		}

		@Bean
		public Function<String, String> uppercase() {
			return v -> v.toUpperCase();
		}

		@Bean
		public Function<String, List<Message<Person>>> persons() {
			return x -> {
				Person person = new Person();
				person.setName(x);
				person.setId(3);
				return Collections.singletonList(MessageBuilder.withPayload(person).build());
			};
		}

		@Bean
		public Function<Flux<String>, Flux<Person>> funcReactive() {
			return flux -> flux.map(value -> {
				Person person = new Person();
				person.setName(value);
				person.setId(3);
				return person;
			});
		}
	}

	@EnableAutoConfiguration
	public static class ExplicitChannelConfiguration {

		@Bean("echo-in-0")
		public SubscribableChannel myChannel() {
			return new DirectWithAttributesChannel();
		}

		@Bean
		public Function<String, String> echo() {
			return x -> x;
		}
	}

	@EnableAutoConfiguration
	public static class FailedApplicationListenerConfiguration {

		@Bean
		public Function<String, String> echo() {
			return x -> x;
		}

		@Bean
		public ApplicationListener<BindingCreatedEvent> bindingCreatedEventListener() {
			return bindingCreatedEvent -> {
				throw new RuntimeException("Test");
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleConsumerWithMultipleDestinationConfiguration {

		@Bean
		public Consumer<String> consumerMultiple() {
			return value -> {
				System.out.println(value);
			};
		}
	}

	@EnableAutoConfiguration
	public static class WrappedBiConsumerAutoConfiguration {

		@Bean
		public BiConsumer<String, Map<Object, String>> testBiConsumer() {
			return (a, b) -> { };
		}

	}

	public static class Person {
		private String name;
		private int id;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}
	}

}
