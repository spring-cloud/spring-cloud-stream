/*
 * Copyright 2018-2020 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamBridgeTests.EmptyConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * This test compares execution time of the StreamBridge send method with or without proper caching of the output conversion functions.
 */
public class StreamBridgeSendTests {

	private static final int NUMBER_OF_RUNS = 100000;
	private StreamBridge bridge;
	private BindingServiceProperties bindingServiceProperties;

	@Before
	public void before() {
		System.clearProperty("spring.cloud.function.definition");

		ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						EmptyConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.cloud.stream.source=foo;bar",
								"--spring.cloud.stream.bindings.foo-out-0.producer.partitionKeyExpression=payload",
								"--spring.cloud.stream.bindings.foo-out-0.producer.partitionCount=2",
								"--spring.jmx.enabled=false");

		bridge = context.getBean(StreamBridge.class);
		bindingServiceProperties = context.getBean(BindingServiceProperties.class);
	}

	@Test
	public void invokeSendWithoutCache() {
		bindingServiceProperties.setDynamicDestinationCacheSize(0);

		System.err.println(String.format("NoCache send called %s times took %s ms.",
				NUMBER_OF_RUNS, invokeAndMeasureSend()));
	}

	@Test
	public void invokeSendWithCache() {
		bindingServiceProperties.setDynamicDestinationCacheSize(10);

		System.err.println(String.format("Cached send called %s times took %s ms.",
				NUMBER_OF_RUNS, invokeAndMeasureSend()));
	}

	private float invokeAndMeasureSend() {
		long startTime = System.nanoTime();

		for (int i = 0; i < NUMBER_OF_RUNS; i++) {
			bridge.send("foo-out-0", "a");
		}

		long endTime = System.nanoTime();
		return (endTime - startTime) / 1000000.0f;

	}
}

