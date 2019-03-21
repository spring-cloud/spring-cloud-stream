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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Flux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
@RunWith(Parameterized.class)
public class StreamListenerReactiveReturnWithPojoTests {

	private Class<?> configClass;

	private ObjectMapper mapper = new ObjectMapper();

	public StreamListenerReactiveReturnWithPojoTests(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection<?> InputConfigs() {
		return Arrays.asList(ReactorTestReturnWithPojo1.class,
				ReactorTestReturnWithPojo2.class, ReactorTestReturnWithPojo3.class,
				ReactorTestReturnWithPojo4.class);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReturnWithPojo() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
				"--server.port=0", "--spring.jmx.enabled=false");
		Processor processor = context.getBean(Processor.class);
		processor.input().send(MessageBuilder.withPayload("{\"message\":\"helloPojo\"}")
				.setHeader("contentType", "application/json").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		BarPojo barPojo = this.mapper.readValue(result.getPayload(), BarPojo.class);
		assertThat(barPojo.getBarMessage()).isEqualTo("helloPojo");
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestReturnWithPojo1 {

		@StreamListener
		public @Output(Processor.OUTPUT) Flux<BarPojo> receive(
				@Input(Processor.INPUT) Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestReturnWithPojo2 {

		@StreamListener(Processor.INPUT)
		public @Output(Processor.OUTPUT) Flux<BarPojo> receive(Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestReturnWithPojo3 {

		@StreamListener(Processor.INPUT)
		public @SendTo(Processor.OUTPUT) Flux<BarPojo> receive(Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestReturnWithPojo4 {

		@StreamListener
		public @SendTo(Processor.OUTPUT) Flux<BarPojo> receive(
				@Input(Processor.INPUT) Flux<FooPojo> input) {
			return input.map(m -> new BarPojo(m.getMessage()));
		}

	}

	public static class FooPojo {

		private String message;

		public String getMessage() {
			return this.message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

	}

	public static class BarPojo {

		private String barMessage;

		@JsonCreator
		public BarPojo(@JsonProperty("barMessage") String barMessage) {
			this.barMessage = barMessage;
		}

		public String getBarMessage() {
			return this.barMessage;
		}

		public void setBarMessage(String barMessage) {
			this.barMessage = barMessage;
		}

	}

}
