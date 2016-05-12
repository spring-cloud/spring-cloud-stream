/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.aggregation;

import org.junit.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.aggregate.SharedChannelRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindableChannelFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author Marius Bogoevici
 */
public class ModuleAggregationTest {

	@Test
	public void testModuleAggregation() {
		ConfigurableApplicationContext aggregatedApplicationContext =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class)
						.from(TestSource.class)
						.to(TestProcessor.class)
						.run();
		SharedChannelRegistry sharedChannelRegistry = aggregatedApplicationContext.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory = aggregatedApplicationContext.getBean(BindableChannelFactory.class);
		assertNotNull(channelFactory);
		assertThat(sharedChannelRegistry.getAll().keySet(), hasSize(2));
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespaces() {
		ConfigurableApplicationContext aggregatedApplicationContext =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class)
						.from(TestSource.class)
						.namespace("foo")
						.to(TestProcessor.class)
						.namespace("bar")
						.run();
		SharedChannelRegistry sharedChannelRegistry
				= aggregatedApplicationContext.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory
				= aggregatedApplicationContext.getBean(BindableChannelFactory.class);
		Object fooOutput = sharedChannelRegistry.get("foo.output");
		assertNotNull(fooOutput);
		assertThat(fooOutput, instanceOf(MessageChannel.class));
		Object barInput = sharedChannelRegistry.get("bar.input");
		assertNotNull(barInput);
		assertThat(barInput, instanceOf(MessageChannel.class));
		assertNotNull(channelFactory);
		assertThat(sharedChannelRegistry.getAll().keySet(), hasSize(2));
		aggregatedApplicationContext.close();
	}


	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	public static class TestSource {

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestProcessor {

	}

}
