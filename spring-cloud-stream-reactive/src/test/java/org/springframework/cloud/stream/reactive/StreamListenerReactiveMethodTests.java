/*
 * Copyright 2016-2019 the original author or authors.
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

import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM;
import static org.springframework.cloud.stream.binding.StreamListenerErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED;

/**
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerReactiveMethodTests {

	@Test
	public void testReactiveInvalidInputValueWithOutputMethodParameters() {
		try {
			SpringApplication.run(ReactorTestInputOutputArgs.class, "--server.port=0");
			fail("IllegalArgumentException should have been thrown");
		}
		catch (Exception e) {
			assertThat(e.getMessage())
					.contains(INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM);
		}
	}

	@Test
	public void testMethodReturnTypeWithNoOutboundSpecified() {
		try {
			SpringApplication.run(ReactorTestReturn5.class, "--server.port=0",
					"--spring.jmx.enabled=false",
					"--spring.cloud.stream.bindings.input.contentType=text/plain",
					"--spring.cloud.stream.bindings.output.contentType=text/plain");
			fail("Exception expected: " + RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
		catch (Exception e) {
			assertThat(e.getMessage()).contains(RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestInputOutputArgs {

		@StreamListener(Processor.INPUT)
		public void receive(Flux<String> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestReturn5 {

		@StreamListener
		public Flux<String> receive(@Input(Processor.INPUT) Flux<String> input) {
			return input.map(m -> m.toUpperCase());
		}

	}

}
