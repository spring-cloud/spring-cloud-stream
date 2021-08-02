/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.deployer;

import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.messaging.Message;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.support.MessageBuilder;


/**
 *
 * @author Oleg Zhurakousky
 * @since 3.1.2
 *
 */
@SpringBootApplication
public class RabbitDeployer {

	/*
	 * SpringApplication.run(RabbitDeployer.class,
				"--spring.cloud.function.definition=reverseFunction",
				"--spring.cloud.function.location=/bootjar-1.0.0.RELEASE-exec.jar",
				"--spring.cloud.function.function-class=function.example.ReverseFunction"
				);
	 */
	public static void main(String[] args) {
		SpringApplication.run(RabbitDeployer.class);
	}

	@Bean
	@ConditionalOnProperty(name = FunctionProperties.PREFIX + ".rsocket.enabled", matchIfMissing = true)
	public Function<Message<byte[]>, Message<byte[]>> gateway(StreamBridge bridge) {
		return message -> {
			String destinationName = (String) message.getHeaders().get("target_destination");
			bridge.send(destinationName, message);
			return MessageBuilder.withPayload("Successfully sent to reverseFunction-in-0".getBytes()).build();
		};
	}

	/*
	 * Just like any other stream bean. This one will subscribe to broker destination (using regular stream mechanisms)
	 * and using some configuration provided by the user will propagate message to remote (rsocket) subscriber
	 */
	@Bean
	@ConditionalOnProperty(name = FunctionProperties.PREFIX + ".rsocket.enabled", matchIfMissing = true)
	public Consumer<Message<byte[]>> delegatingConsumer(RSocketRequester.Builder rsocketRequesterBuilder, Environment environment) {
		String host = environment.getProperty("spring.cloud.function.rsocket.subscriber.host");
		String port = environment.getProperty("spring.cloud.function.rsocket.subscriber.port");
		return message -> {
//			rsocketRequesterBuilder.tcp("host", Integer.valueOf(port))
//			.route("pojoToString")
//			.data(message)
//			.retrieveMono(String.class);
		};
	}

	// Step-1 - rabbit-bundle.jar(time) | rabbit-bundle.jar(log) - Step One - local
	// Step-2 - polyglot
	// Step-3 - SCDF
	// Step-4 - Kubernetes

	//http | rabbit-rsocket-bundle.jar(producer) | python | rabbit-rsocket-bundle.jar(consumer) | rabbit-bundle.jar(log)

}
