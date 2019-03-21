/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.time.Duration;

import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.cloud.stream.binding.StreamAnnotationErrorMessages.ATLEAST_ONE_OUTPUT;
import static org.springframework.cloud.stream.binding.StreamAnnotationErrorMessages.INVALID_OUTBOUND_NAME;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.CANNOT_CONVERT_RETURN_TYPE_TO_ANY_AVAILABLE_RESULT_ADAPTERS;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.INPUT_ANNOTATIONS_ARE_NOT_ALLOWED;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.INVALID_OUTPUT_METHOD_PARAMETERS;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.NO_OUTPUT_SPECIFIED;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.OUTPUT_ANNOTATION_MISSING_ON_METHOD_PARAMETERS_VOID_RETURN_TYPE;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.RETURN_TYPE_METHOD_ARGUMENTS;
import static org.springframework.cloud.stream.reactive.StreamEmitterErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED;

/**
 * @author Soby Chacko
 * @author Vinicius Carvalho
 */
public class StreamEmitterValidationTests {

	@Test
	public void testOutputAsMethodandMethodParameter() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestOutputAsMethodandMethodParameter.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + INVALID_OUTPUT_METHOD_PARAMETERS);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(INVALID_OUTPUT_METHOD_PARAMETERS);
		}
	}

	@Test
	public void testFluxReturnTypeNoOutputGiven() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestFluxReturnTypeNoOutputGiven.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + NO_OUTPUT_SPECIFIED);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(NO_OUTPUT_SPECIFIED);
		}
	}

	@Test
	public void testVoidReturnTypeNoOutputGiven() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestVoidReturnTypeNoOutputGiven.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + NO_OUTPUT_SPECIFIED);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(NO_OUTPUT_SPECIFIED);
		}
	}

	@Test
	public void testNonVoidReturnButOutputAsMethodParameter() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestNonVoidReturnButOutputAsMethodParameter.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
	}

	@Test
	public void testNonVoidReturnButMethodArguments() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestNonVoidReturnButMethodArguments.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + RETURN_TYPE_METHOD_ARGUMENTS);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(RETURN_TYPE_METHOD_ARGUMENTS);
		}
	}

	@Test
	public void testVoidReturnTypeMultipleMethodParametersWithOneMissingOutput() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(
					TestVoidReturnTypeMultipleMethodParametersWithOneMissingOutput.class);
			context.refresh();
			context.close();
			fail("Expected exception: "
					+ OUTPUT_ANNOTATION_MISSING_ON_METHOD_PARAMETERS_VOID_RETURN_TYPE);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(
					OUTPUT_ANNOTATION_MISSING_ON_METHOD_PARAMETERS_VOID_RETURN_TYPE);
		}
	}

	@Test
	public void testOutputAtCorrectLocationButNameMissing1() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestOutputAtCorrectLocationButNameMissing1.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + ATLEAST_ONE_OUTPUT);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(ATLEAST_ONE_OUTPUT);
		}
	}

	@Test
	public void testOutputAtCorrectLocationButNameMissing2() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestOutputAtCorrectLocationButNameMissing2.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + INVALID_OUTBOUND_NAME);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(INVALID_OUTBOUND_NAME);
		}
	}

	@Test
	public void testInputAnnotationsAreNotPermitted() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestInputAnnotationsAreNotPermitted.class);
			context.refresh();
			context.close();
			fail("Expected exception: " + INPUT_ANNOTATIONS_ARE_NOT_ALLOWED);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(INPUT_ANNOTATIONS_ARE_NOT_ALLOWED);
		}
	}

	@Test
	public void testReturnTypeNotSupported() {
		try {
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
			context.register(TestReturnTypeNotSupported.class);
			context.refresh();
			context.close();
			fail("Expected exception: "
					+ CANNOT_CONVERT_RETURN_TYPE_TO_ANY_AVAILABLE_RESULT_ADAPTERS);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(
					CANNOT_CONVERT_RETURN_TYPE_TO_ANY_AVAILABLE_RESULT_ADAPTERS);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestOutputAsMethodandMethodParameter {

		@StreamEmitter
		@Output(Source.OUTPUT)
		public void receive(@Output(Source.OUTPUT) FluxSender output) {
			output.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestFluxReturnTypeNoOutputGiven {

		@StreamEmitter
		public Flux<String> emit() {
			return Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l);
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestVoidReturnTypeNoOutputGiven {

		@StreamEmitter
		public void emit(FluxSender output) {
			output.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestNonVoidReturnButOutputAsMethodParameter {

		@StreamEmitter
		public Flux<String> emit(@Output(Source.OUTPUT) FluxSender output) {
			return Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l);
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestNonVoidReturnButMethodArguments {

		@StreamEmitter
		@Output(Source.OUTPUT)
		public Flux<String> receive(FluxSender output) {
			return Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l);
		}

	}

	@EnableBinding(StreamEmitterBasicTests.TestMultiOutboundChannels.class)
	@EnableAutoConfiguration
	public static class TestVoidReturnTypeMultipleMethodParametersWithOneMissingOutput {

		@StreamEmitter
		public void emit(
				@Output(StreamEmitterBasicTests.TestMultiOutboundChannels.OUTPUT1) FluxSender output1,
				@Output(StreamEmitterBasicTests.TestMultiOutboundChannels.OUTPUT2) FluxSender output2,
				FluxSender output3) {
			output1.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
			output2.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
			output3.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestOutputAtCorrectLocationButNameMissing1 {

		@StreamEmitter
		@Output("")
		public void receive(FluxSender output) {
			output.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
		}

	}

	@EnableBinding(StreamEmitterBasicTests.TestMultiOutboundChannels.class)
	@EnableAutoConfiguration
	public static class TestOutputAtCorrectLocationButNameMissing2 {

		@StreamEmitter
		public void emit(@Output("") FluxSender output1) {
			output1.send(
					Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestInputAnnotationsAreNotPermitted {

		@StreamEmitter
		@Output(Source.OUTPUT)
		@Input(Processor.INPUT)
		public Flux<String> emit() {
			return Flux.interval(Duration.ofMillis(1)).map(l -> "Hello World!!" + l);
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestReturnTypeNotSupported {

		@StreamEmitter
		@Output(Source.OUTPUT)
		public String emit() {
			return "hello";
		}

	}

}
