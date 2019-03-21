/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.aggregation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder.SourceConfigurer;
import org.springframework.cloud.stream.aggregate.SharedBindingTargetRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BindingTargetFactory;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Artem Bilan
 * @author Janne Valkealahti
 * @author Gary Russell
 * @author Soby Chacko
 */
@Ignore
public class AggregationTest {

	private ConfigurableApplicationContext aggregatedApplicationContext;

	@After
	public void closeContext() {
		System.clearProperty("a.foo-value");
		System.clearProperty("c.fooValue");
		System.clearProperty("a.foo.value");
		System.clearProperty("c.foo.value");
		if (this.aggregatedApplicationContext != null) {
			this.aggregatedApplicationContext.close();
		}
	}

	@Test
	public void aggregation() {
		this.aggregatedApplicationContext = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock")
						.web(false).from(TestSource.class).to(TestProcessor.class).run();
		SharedBindingTargetRegistry sharedBindingTargetRegistry = this.aggregatedApplicationContext
				.getBean(SharedBindingTargetRegistry.class);
		BindingTargetFactory channelFactory = this.aggregatedApplicationContext
				.getBean(SubscribableChannelBindingTargetFactory.class);
		assertThat(channelFactory).isNotNull();
		assertThat(sharedBindingTargetRegistry.getAll().keySet()).hasSize(2);
		this.aggregatedApplicationContext.close();
	}

	@Test
	public void testModuleAggregationUsingSharedChannelRegistry() {
		// test backward compatibility
		this.aggregatedApplicationContext = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock")
						.web(false).from(TestSource.class).to(TestProcessor.class).run();
		SharedBindingTargetRegistry sharedChannelRegistry = this.aggregatedApplicationContext
				.getBean(SharedBindingTargetRegistry.class);
		BindingTargetFactory channelFactory = this.aggregatedApplicationContext
				.getBean(SubscribableChannelBindingTargetFactory.class);
		assertThat(channelFactory).isNotNull();
		assertThat(sharedChannelRegistry.getAll().keySet()).hasSize(2);
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParentArgsAndSources() {

		List<String> argsToVerify = new ArrayList<>();
		argsToVerify.add("--foo1=bar1");
		argsToVerify.add("--foo2=bar2");
		argsToVerify.add("--foo3=bar3");
		argsToVerify.add("--spring.cloud.stream.default-binder=mock");
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--foo1=bar1");
		final ConfigurableApplicationContext context = aggregateApplicationBuilder
				.parent(DummyConfig.class, "--foo2=bar2").web(false)
				.from(TestSource.class).namespace("foo").to(TestProcessor.class)
				.namespace("bar")
				.run("--foo3=bar3", "--spring.cloud.stream.default-binder=mock");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		final List<String> parentArgs = (List<String>) aggregateApplicationBuilderAccessor
				.getPropertyValue("parentArgs");
		assertThat(parentArgs).containsExactlyInAnyOrder(
				argsToVerify.toArray(new String[argsToVerify.size()]));
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParentArgsAndSourcesWithWebDisabled() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--foo1=bar1");
		final ConfigurableApplicationContext context = aggregateApplicationBuilder
				.parent(DummyConfig.class, "--foo2=bar2").web(false)
				.from(TestSource.class).namespace("foo").to(TestProcessor.class)
				.namespace("bar").run("--spring.cloud.stream.default-binder=mock");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		List<Object> sources = (List<Object>) aggregateApplicationBuilderAccessor
				.getPropertyValue("parentSources");
		assertThat(sources).containsExactlyInAnyOrder(
				AggregateApplicationBuilder.ParentConfiguration.class,
				AggregateApplicationBuilder.ParentActuatorConfiguration.class,
				AggregationAppConfig.class, DummyConfig.class);
		context.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesFromCmdLine() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").via(TestProcessor.class).namespace("b")
				.via(TestProcessor.class).namespace("c")
				.run("--a.foo1=bar1", "--b.foo1=bar2", "--c.foo1=bar3");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		assertThat(Arrays
				.asList(((SourceConfigurer) aggregateApplicationBuilderAccessor
						.getPropertyValue("sourceConfigurer")).getArgs())
				.contains("--foo1=bar1")).isTrue();
		final List<AggregateApplicationBuilder.ProcessorConfigurer> processorConfigurers;
		processorConfigurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : processorConfigurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=bar2" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays.asList(processorConfigurer.getArgs())
						.contains("--foo1=bar3")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesFromCmdLineVsArgs() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").args("--fooValue=bar").via(TestProcessor.class)
				.namespace("b").args("--foo1=argbarb").via(TestProcessor.class)
				.namespace("c").run("--a.fooValue=bara", "--c.foo1=barc");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		assertThat(Arrays
				.asList(((SourceConfigurer) aggregateApplicationBuilderAccessor
						.getPropertyValue("sourceConfigurer")).getArgs())
				.contains("--fooValue=bara")).isTrue();
		final List<AggregateApplicationBuilder.ProcessorConfigurer> processorConfigurers;
		processorConfigurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : processorConfigurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo1=argbarb" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays
						.asList(((SourceConfigurer) aggregateApplicationBuilderAccessor
								.getPropertyValue("sourceConfigurer")).getArgs())
						.contains("--fooValue=bara")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesFromCmdLineWithRelaxedNames() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").args("--foo-value=bar").via(TestProcessor.class)
				.namespace("b").args("--fooValue=argbarb").via(TestProcessor.class)
				.namespace("c")
				.run("--a.fooValue=bara", "--b.foo-value=barb", "--c.foo1=barc");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		assertThat(Arrays
				.asList(((SourceConfigurer) aggregateApplicationBuilderAccessor
						.getPropertyValue("sourceConfigurer")).getArgs())
				.contains("--fooValue=bara")).isTrue();
		final List<AggregateApplicationBuilder.ProcessorConfigurer> processorConfigurers;
		processorConfigurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : processorConfigurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--foo-value=barb" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays.asList(processorConfigurer.getArgs())
						.contains("--foo1=barc")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesFromCmdLineWithRelaxedNamesAndMorePropertySources() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		System.setProperty("a.foo-value", "sysbara");
		System.setProperty("c.fooValue", "sysbarc");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").args("--foo-value=bar").via(TestProcessor.class)
				.namespace("b").args("--fooValue=argbarb").via(TestProcessor.class)
				.namespace("c").args("--foo-value=argbarc").run("--a.fooValue=bara");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		SourceConfigurer sourceConfigurer = (SourceConfigurer) aggregateApplicationBuilderAccessor
				.getPropertyValue("sourceConfigurer");
		assertThat(Arrays.asList(sourceConfigurer.getArgs()).contains("--fooValue=bara"))
				.isTrue();
		assertThat(Arrays.asList(sourceConfigurer.getArgs()).contains("--foo-value=bara"))
				.isTrue();
		final List<AggregateApplicationBuilder.ProcessorConfigurer> processorConfigurers;
		processorConfigurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : processorConfigurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays.asList(processorConfigurer.getArgs())
						.contains("--fooValue=sysbarc")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesWithoutCmdLinePropertySource() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		System.setProperty("a.foo-value", "sysbara");
		System.setProperty("c.fooValue", "sysbarc");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").args("--foo-value=bar").via(TestProcessor.class)
				.namespace("b").args("--fooValue=argbarb").via(TestProcessor.class)
				.namespace("c").args("--foo-value=argbarc").run();
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		SourceConfigurer sourceConfigurer = (SourceConfigurer) aggregateApplicationBuilderAccessor
				.getPropertyValue("sourceConfigurer");
		assertThat(
				Arrays.asList(sourceConfigurer.getArgs()).contains("--foo-value=sysbara"))
						.isTrue();
		List<AggregateApplicationBuilder.ProcessorConfigurer> configurers;
		configurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : configurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays.asList(processorConfigurer.getArgs())
						.contains("--fooValue=sysbarc")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNamespacePrefixesWithCAPSProperties() {
		AggregateApplicationBuilder aggregateApplicationBuilder = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock");
		System.setProperty("a.fooValue", "sysbara");
		System.setProperty("c.fooValue", "sysbarc");
		this.aggregatedApplicationContext = aggregateApplicationBuilder
				.parent(DummyConfig.class).web(false).from(TestSource.class)
				.namespace("a").args("--foo-value=bar").via(TestProcessor.class)
				.namespace("b").args("--fooValue=argbarb").via(TestProcessor.class)
				.namespace("c").args("--foo-value=argbarc").run("--a.fooValue=highest");
		DirectFieldAccessor aggregateApplicationBuilderAccessor = new DirectFieldAccessor(
				aggregateApplicationBuilder);
		String[] configurers = ((SourceConfigurer) aggregateApplicationBuilderAccessor
				.getPropertyValue("sourceConfigurer")).getArgs();
		assertThat(configurers).contains(new String[] { "--fooValue=highest" });
		final List<AggregateApplicationBuilder.ProcessorConfigurer> processorConfigurers;
		processorConfigurers = (List<AggregateApplicationBuilder.ProcessorConfigurer>) aggregateApplicationBuilderAccessor
				.getPropertyValue("processorConfigurers");
		for (AggregateApplicationBuilder.ProcessorConfigurer processorConfigurer : processorConfigurers) {
			if (processorConfigurer.getNamespace().equals("b")) {
				assertThat(Arrays.equals(processorConfigurer.getArgs(),
						new String[] { "--fooValue=argbarb" })).isTrue();
			}
			if (processorConfigurer.getNamespace().equals("c")) {
				assertThat(Arrays.asList(processorConfigurer.getArgs())
						.contains("--fooValue=sysbarc")).isTrue();
			}
		}
		this.aggregatedApplicationContext.close();
	}

	@Test
	public void testNamespaces() {
		this.aggregatedApplicationContext = new AggregateApplicationBuilder(
				AggregationAppConfig.class, "--spring.cloud.stream.default-binder=mock")
						.web(false).from(TestSource.class).namespace("foo")
						.to(TestProcessor.class).namespace("bar").run();
		SharedBindingTargetRegistry sharedChannelRegistry = this.aggregatedApplicationContext
				.getBean(SharedBindingTargetRegistry.class);
		BindingTargetFactory channelFactory = this.aggregatedApplicationContext
				.getBean(SubscribableChannelBindingTargetFactory.class);
		MessageChannel fooOutput = sharedChannelRegistry.get("foo.output",
				MessageChannel.class);
		assertThat(fooOutput).isNotNull();
		Object barInput = sharedChannelRegistry.get("bar.input", MessageChannel.class);
		assertThat(barInput).isNotNull();
		assertThat(channelFactory).isNotNull();
		assertThat(sharedChannelRegistry.getAll().keySet()).hasSize(2);
		this.aggregatedApplicationContext.close();
	}

	@Test
	public void testBindableProxyFactoryCaching() {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestSource2.class, TestProcessor.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.default-binder=mock");

		Map<String, BindableProxyFactory> factories = context
				.getBeansOfType(BindableProxyFactory.class);
		assertThat(factories).hasSize(2);

		Map<String, Source> sources = context.getBeansOfType(Source.class);
		assertThat(sources).hasSize(1);
		for (Source source : sources.values()) {
			source.output();
		}

		Map<String, FooSource> fooSources = context.getBeansOfType(FooSource.class);
		assertThat(fooSources).hasSize(1);
		for (FooSource source : fooSources.values()) {
			source.output();
		}

		Map<String, Processor> processors = context.getBeansOfType(Processor.class);
		assertThat(processors).hasSize(1);
		for (Processor processor : processors.values()) {
			processor.input();
			processor.output();
		}

		for (BindableProxyFactory factory : factories.values()) {
			Field field = ReflectionUtils.findField(BindableProxyFactory.class,
					"targetCache");
			ReflectionUtils.makeAccessible(field);
			Map<?, ?> targetCache = (Map<?, ?>) ReflectionUtils.getField(field, factory);
			if (factory.getObjectType() == Source.class) {
				assertThat(targetCache).hasSize(1);
			}
			if (factory.getObjectType() == FooSource.class) {
				assertThat(targetCache).hasSize(1);
			}
			else if (factory.getObjectType() == Processor.class) {
				assertThat(targetCache).hasSize(2);
			}
			else {
				fail("Found unexpected type");
			}
		}
		context.close();
	}

	public interface FooSource {

		@Output("fooOutput")
		MessageChannel output();

	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	public static class TestSource {

	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestProcessor {

	}

	@EnableBinding(FooSource.class)
	@EnableAutoConfiguration
	public static class TestSource2 {

	}

	@Configuration
	public static class DummyConfig {

	}

	@Configuration
	@EnableAutoConfiguration
	public static class AggregationAppConfig {

	}

}
