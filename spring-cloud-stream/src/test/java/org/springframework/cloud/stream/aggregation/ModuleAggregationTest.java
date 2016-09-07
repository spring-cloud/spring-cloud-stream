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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ModuleAggregationTest {

	@Test
	public void testModuleAggregation() {
		ConfigurableApplicationContext aggregatedApplicationContext =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class,
						"--server.port=0")
						.from(TestSource.class)
						.to(TestProcessor.class)
						.run();
		SharedChannelRegistry sharedChannelRegistry = aggregatedApplicationContext.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory = aggregatedApplicationContext.getBean(BindableChannelFactory.class);
		assertThat(channelFactory).isNotNull();
		assertThat(sharedChannelRegistry.getAll().keySet()).hasSize(2);
		aggregatedApplicationContext.close();
	}

	@Test
	public void testParentArgsAndSources() {
		List<String> argsToVerify = new ArrayList<>();
		argsToVerify.add("--foo1=bar1");
		argsToVerify.add("--foo2=bar2");
		argsToVerify.add("--foo3=bar3");
		AggregateApplicationBuilder aggregateApplicationBuilder =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class,
						"--foo1=bar1");
		aggregateApplicationBuilder.parent(DummyConfig.class, "--foo2=bar2")
				.from(TestSource.class)
				.namespace("foo").to(TestProcessor.class).namespace("bar")
				.run("--foo3=bar3");
		Field parentArgsField = ReflectionUtils.findField(AggregateApplicationBuilder.class,"parentArgs", List.class);
		ReflectionUtils.makeAccessible(parentArgsField);
		Field parentSourcesField = ReflectionUtils.findField(AggregateApplicationBuilder.class,"parentSources", List.class);
		ReflectionUtils.makeAccessible(parentSourcesField);
		String args = ReflectionUtils.getField(parentArgsField, aggregateApplicationBuilder).toString();
		Assert.assertEquals(args, argsToVerify.toString());
		List<Object> sources = ((List<Object>)ReflectionUtils.getField(parentSourcesField, aggregateApplicationBuilder));
		Assert.assertTrue(sources.size() == 3);
		Assert.assertTrue(sources.contains(AggregateApplicationBuilder.ParentConfiguration.class));
		Assert.assertTrue(sources.contains(MockBinderRegistryConfiguration.class));
		Assert.assertTrue(sources.contains(DummyConfig.class));
	}

	@Test
	public void testNamespaces() {
		ConfigurableApplicationContext aggregatedApplicationContext =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class,
						"--server.port=0")
						.from(TestSource.class)
						.namespace("foo").to(TestProcessor.class).namespace("bar")
						.run();
		SharedChannelRegistry sharedChannelRegistry
				= aggregatedApplicationContext.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory
				= aggregatedApplicationContext.getBean(BindableChannelFactory.class);
		Object fooOutput = sharedChannelRegistry.get("foo.output");
		assertThat(fooOutput).isNotNull();
		assertThat(fooOutput).isInstanceOf(MessageChannel.class);
		Object barInput = sharedChannelRegistry.get("bar.input");
		assertThat(barInput).isNotNull();
		assertThat(barInput).isInstanceOf(MessageChannel.class);
		assertThat(channelFactory).isNotNull();
		assertThat(sharedChannelRegistry.getAll().keySet()).hasSize(2);
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

	@Configuration
	public static class DummyConfig {

	}
}
