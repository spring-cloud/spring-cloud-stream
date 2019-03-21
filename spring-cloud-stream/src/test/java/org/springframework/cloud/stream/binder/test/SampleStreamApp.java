/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.nio.charset.StandardCharsets;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sample spring cloud stream application that demonstrates the usage of
 * {@link TestChannelBinder}.
 *
 * @author Oleg Zhurakousky
 * @author Gary Russell
 *
 */
@SpringBootApplication
@EnableBinding(SampleStreamApp.PolledConsumer.class)
@Import(TestChannelBinderConfiguration.class)
public class SampleStreamApp {

	public static void main(String[] args) {
		ApplicationContext context = new SpringApplicationBuilder(SampleStreamApp.class)
				.web(WebApplicationType.NONE).run("--server.port=0");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("Hello".getBytes()));

		Message<?> message = target.receive();
		assertThat(new String((byte[]) message.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("Hello");
	}

	@Bean
	public ApplicationRunner runner(PollableMessageSource pollableSource) {
		return args -> pollableSource.poll(message -> {
			System.out.println("Polled payload: " + message.getPayload());
		});
	}

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public String receive(String value) {
		System.out.println("Handling payload: " + value);
		return value;
	}

	@ServiceActivator(inputChannel = "input.anonymous.errors")
	public void error(String value) {
		System.out.println("Handling ERROR payload: " + value);
	}

	public interface PolledConsumer extends Processor {

		@Input
		PollableMessageSource pollableSource();

	}

}
