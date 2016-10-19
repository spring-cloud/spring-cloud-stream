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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder.SourceConfigurer;
import org.springframework.cloud.stream.aggregate.SharedChannelRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindableChannelFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ModuleAggregationTest {

	private ConfigurableApplicationContext aggregatedApplicationContext;

	@After
	public void closeContext() {
		System.clearProperty("a.foo-value");
		System.clearProperty("c.fooValue");
		System.clearProperty("a_FOO_VALUE");
		System.clearProperty("C_FOO_VALUE");
		if (aggregatedApplicationContext != null) {
			aggregatedApplicationContext.close();
		}
	}

	@Test
	public void testModuleAggregation() {
		aggregatedApplicationContext = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class, "--server.port=0")
				.from(TestSource.class).to(TestProcessor.class).run();
		SharedChannelRegistry sharedChannelRegistry = aggregatedApplicationContext
				.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory = aggregatedApplicationContext
				.getBean(BindableChannelFactory.class);
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
		argsToVerify.add("--server.port=0");
		AggregateApplicationBuilder aggregateApplicationBuilder =
				new AggregateApplicationBuilder(MockBinderRegistryConfiguration.class,
						"--foo1=bar1");
		final ConfigurableApplicationContext context =
				aggregateApplicationBuilder.parent(DummyConfig.class, "--foo2=bar2")
						.from(TestSource.class)
						.namespace("foo").to(TestProcessor.class).namespace("bar")
						.run("--foo3=bar3", "--server.port=0");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertThat((List<String>) aggregateApplicationBuilderAccessor.getPropertyValue(
				"parentArgs")).containsExactlyInAnyOrder(argsToVerify.toArray(new String[argsToVerify.size()]));
		List<Object> sources = (List<Object>) aggregateApplicationBuilderAccessor.getPropertyValue("parentSources");
		assertThat(sources).containsExactlyInAnyOrder(AggregateApplicationBuilder.ParentConfiguration.class,
				MockBinderRegistryConfiguration.class, DummyConfig.class);
		context.close();
	}

	@Test
	public void testNamespacePrefixesFromCmdLine() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").via(TestProcessor.class).namespace("b")
				.via(TestProcessor.class).namespace("c")
				.run("--a.foo1=bar1", "--b.foo1=bar2", "--c.foo1=bar3");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--foo1=bar1"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=bar2" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=bar3" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespacePrefixesFromCmdLineVsArgs() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").args("--fooValue=bar")
				.via(TestProcessor.class).namespace("b").args("--foo1=argbarb")
				.via(TestProcessor.class).namespace("c")
				.run("--a.fooValue=bara", "--c.foo1=barc");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--fooValue=bara"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=argbarb" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=barc" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespacePrefixesFromCmdLineWithRelaxedNames() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").args("--foo-value=bar")
				.via(TestProcessor.class).namespace("b").args("--fooValue=argbarb")
				.via(TestProcessor.class).namespace("c")
				.run("--a.fooValue=bara", "--b.foo-value=barb", "--c.foo1=barc");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--fooValue=bara"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo-value=barb" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=barc" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespacePrefixesFromCmdLineWithRelaxedNamesAndMorePropertySources() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		System.setProperty("a.foo-value", "sysbara");
		System.setProperty("c.fooValue", "sysbarc");
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").args("--foo-value=bar")
				.via(TestProcessor.class).namespace("b").args("--fooValue=argbarb")
				.via(TestProcessor.class).namespace("c").args("--foo-value=argbarc")
				.run("--a.fooValue=bara");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--fooValue=bara"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=sysbarc" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespacePrefixesWithoutCmdLinePropertySource() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		System.setProperty("a.foo-value", "sysbara");
		System.setProperty("c.fooValue", "sysbarc");
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").args("--foo-value=bar")
				.via(TestProcessor.class).namespace("b").args("--fooValue=argbarb")
				.via(TestProcessor.class).namespace("c").args("--foo-value=argbarc")
				.run();
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--foo-value=sysbara"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=sysbarc" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespacePrefixesWithCAPSProperties() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class);
		System.setProperty("a_FOO_VALUE", "sysbara");
		System.setProperty("C_FOO_VALUE", "sysbarc");
		aggregatedApplicationContext = aggregateApplicationBuilder.parent(DummyConfig.class).from(TestSource.class)
				.namespace("a").args("--foo-value=bar")
				.via(TestProcessor.class).namespace("b").args("--fooValue=argbarb")
				.via(TestProcessor.class).namespace("c").args("--foo-value=argbarc")
				.run("--a.fooValue=highest");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(aggregateApplicationBuilder);
		assertTrue(Arrays.equals(((SourceConfigurer) aggregateApplicationBuilderAccessor.getPropertyValue("sourceConfigurer")).getArgs(),
				new String[] {"--fooValue=highest"}));
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : ((List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor.getPropertyValue("processorConfigurers"))) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" }));
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertTrue(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo-value=sysbarc" }));
			}
		}
		aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespaces() {
		aggregatedApplicationContext = new AggregateApplicationBuilder(
				MockBinderRegistryConfiguration.class, "--server.port=0")
				.from(TestSource.class).namespace("foo").to(TestProcessor.class)
				.namespace("bar").run();
		SharedChannelRegistry sharedChannelRegistry = aggregatedApplicationContext
				.getBean(SharedChannelRegistry.class);
		BindableChannelFactory channelFactory = aggregatedApplicationContext
				.getBean(BindableChannelFactory.class);
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
