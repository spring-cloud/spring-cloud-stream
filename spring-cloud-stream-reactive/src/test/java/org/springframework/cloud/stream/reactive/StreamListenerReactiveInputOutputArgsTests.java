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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
@RunWith(Parameterized.class)
public class StreamListenerReactiveInputOutputArgsTests {

	private Class<?> configClass;

	public StreamListenerReactiveInputOutputArgsTests(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection<?> InputConfigs() {
		return Collections.singletonList(ReactorTestInputOutputArgs.class);
	}

	@SuppressWarnings("unchecked")
	private static void sendMessageAndValidate(ConfigurableApplicationContext context)
			throws InterruptedException {
		Processor processor = context.getBean(Processor.class);
		String sentPayload = "hello " + UUID.randomUUID().toString();
		processor.input().send(MessageBuilder.withPayload(sentPayload)
				.setHeader("contentType", "text/plain").build());
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Message<String> result = (Message<String>) messageCollector
				.forChannel(processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isEqualTo(sentPayload.toUpperCase());
	}

	@Test
	public void testInputOutputArgs() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
				"--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.contentType=text/plain",
				"--spring.cloud.stream.bindings.output.contentType=text/plain");
		sendMessageAndValidate(context);
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class ReactorTestInputOutputArgs {

		@StreamListener
		public void receive(@Input(Processor.INPUT) Flux<String> input,
				@Output(Processor.OUTPUT) FluxSender output) {
			output.send(input.map(m -> m.toUpperCase()));
		}

	}

}
