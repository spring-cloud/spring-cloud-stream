/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.contenttype;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.integration.SourceDestination;
import org.springframework.cloud.stream.binder.integration.SpringIntegrationBinderConfiguration;
import org.springframework.cloud.stream.binder.integration.TargetDestination;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;

import static org.junit.Assert.assertEquals;

/**
 * Sort of a TCK test suite to validate payload conversion is
 * done properly by interacting with binder's input/output destinations
 * instead of its bridged channels.
 * This means that all payloads (sent/received) must be expressed in the
 *  wire format (byte[])
 *
 * @author Oleg Zhurakousky
 *
 */
public class BinderConversionTests {
	@Test
	public void testWithJsonAndDefaultChannelType() {
		ApplicationContext context = new SpringApplicationBuilder(ApplicationJsonDefaultType.class).web(false)
				.run("--spring.cloud.stream.default.contentType=application/json", "--spring.jmx.enabled=false");
		SourceDestination source = context.getBean(SourceDestination.class);
		TargetDestination target = context.getBean(TargetDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<byte[]>(jsonPayload.getBytes()));
		assertEquals(jsonPayload, new String((byte[])target.receive().getPayload(), StandardCharsets.UTF_8));
	}

	@SpringBootApplication
	@EnableBinding(Processor.class)
	@Import(SpringIntegrationBinderConfiguration.class)
	public static class ApplicationJsonDefaultType {
		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(Person value) {
			return value;
		}

		public static class Person {
			private String name;

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}
		}
	}
}
