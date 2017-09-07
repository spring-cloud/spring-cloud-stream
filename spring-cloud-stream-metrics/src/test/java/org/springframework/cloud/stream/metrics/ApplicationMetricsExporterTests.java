/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.metrics.config.Emitter;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;

/**
 * @author Vinicius Carvalho
 * @author Janne Valkealahti
 */
public class ApplicationMetricsExporterTests {

	@BeforeClass
	public static void setSystemProps() {
		System.setProperty("spring.test.env.syntax", "testing");
	}

	@AfterClass
	public static void unsetSystemProps() {
		System.clearProperty("spring.test.env.syntax");
	}

	@Test(expected = NoSuchBeanDefinitionException.class)
	public void checkDisabledConfiguration() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0", "--spring.jmx.enabled=false");
		try {
			applicationContext.getBean(Emitter.class);
		}
		catch (Exception e) {
			throw e;
		}
		finally {
			applicationContext.close();
		}

	}

	@Test
	public void defaultIncludes() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper
				.readValue((byte[]) message.getPayload(), ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertEquals("application", applicationMetrics.getName());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void customAppNameAndIndex() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.application.name=foo",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
				ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertEquals("foo", applicationMetrics.getName());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void usingPrefix() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.cloud.stream.metrics.prefix=foo",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper
				.readValue((byte[]) message.getPayload(), ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertEquals("foo.application", applicationMetrics.getName());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void includesExcludesDefaultConfig() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
				"--spring.metrics.export.includes=mem**",
				"--spring.metrics.export.excludes=integration**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper
				.readValue((byte[]) message.getPayload(), ApplicationMetrics.class);
		Assert.assertFalse(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void includesExcludesWithApplicationMetricsConfiguration() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.metrics.export.triggers.application.delay-millis=500",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
				"--spring.metrics.export.triggers.application.includes=mem**",
				"--spring.metrics.export.triggers.application.excludes=integration**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
				ApplicationMetrics.class);
		Assert.assertFalse(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void includesExcludesWithProperties() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
				"--spring.metrics.export.includes=integration**",
				"--spring.cloud.stream.metrics.properties=java**,spring.test.env**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
				ApplicationMetrics.class);
		Assert.assertFalse(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertFalse(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		Assert.assertTrue(applicationMetrics.getProperties().get("spring.test.env.syntax")
				.equals("testing"));
		applicationContext.close();
	}

	@Test
	public void propertiesWithPlaceholdersAndExpressions() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--PLATFORM_APP_NAME=123-name-foo",
				"--PLATFORM_APP_ID=123-id-bar",
				"--spring.cloud.application.guid=${PLATFORM_APP_NAME}.${PLATFORM_APP_ID}",
				"--spring.cloud.application.guid.expression=#{'${PLATFORM_APP_NAME}' + '..' + '${PLATFORM_APP_ID}'}",
				"--spring.cloud.application.guid.default.prop=${app.name.not.found:time-source}",
				"--spring.cloud.application.guid.default.env=${APP_NAME_NOT_FOUND:time-source}",
				"--spring.metrics.export.delay-millis=500",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
				"--spring.metrics.export.includes=integration**",
				"--spring.cloud.stream.metrics.properties=spring**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
				ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assertions.assertThat(applicationMetrics.getProperties().get("spring.cloud.application.guid"))
				.isEqualTo("123-name-foo.123-id-bar");
		Assertions.assertThat(applicationMetrics.getProperties().get("spring.cloud.application.guid.expression"))
				.isEqualTo("123-name-foo..123-id-bar");
		Assertions.assertThat(applicationMetrics.getProperties().get("spring.cloud.application.guid.default.prop"))
				.isEqualTo("time-source");
		Assertions.assertThat(applicationMetrics.getProperties().get("spring.cloud.application.guid.default.env"))
				.isEqualTo("time-source");
		Assert.assertFalse(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		Assert.assertTrue(applicationMetrics.getProperties().get("spring.test.env.syntax").equals("testing"));
		applicationContext.close();
	}

	@Test
	public void propertiesFromLowerPrioritySourcesOverridden() throws Exception {
		System.setProperty("spring.cloud.application.guid.test.metrics", "lowPriority");
		try {
			ConfigurableApplicationContext applicationContext = SpringApplication.run(
					BinderExporterApplication.class,
					"--server.port=0",
					"--spring.jmx.enabled=false",
					"--spring.cloud.application.guid.test.metrics=highPriority",
					"--spring.metrics.export.delay-millis=500",
					"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
					"--spring.metrics.export.includes=integration**",
					"--spring.cloud.stream.metrics.properties=spring**");
			Emitter emitterSource = applicationContext.getBean(Emitter.class);
			MessageCollector collector = applicationContext.getBean(MessageCollector.class);
			Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
					.poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull(message);
			ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
			ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
					ApplicationMetrics.class);
			Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
					applicationMetrics.getMetrics()));
			Assertions.assertThat(applicationMetrics.getProperties().get("spring.cloud.application.guid.test.metrics"))
					.isEqualTo("highPriority");
			applicationContext.close();
		}
		finally {
			System.clearProperty("spring.cloud.application.guid.test.metrics");
		}
	}

	@Test
	public void overrideAppName() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(
				BinderExporterApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false", "--spring.metrics.export.delay-millis=500",
				"--spring.application.name=foo",
				"--spring.cloud.stream.bindings." + Emitter.APPLICATION_METRICS + ".destination=foo",
				"--spring.cloud.stream.metrics.key=foobarfoo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message<?> message = collector.forChannel(emitterSource.applicationMetrics())
				.poll(10, TimeUnit.SECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((byte[]) message.getPayload(),
				ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",
				applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertEquals("foobarfoo", applicationMetrics.getName());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	private boolean contains(String metric, Collection<Metric<?>> metrics) {
		boolean contains = false;
		for (Metric<?> entry : metrics) {
			contains = entry.getName().equals(metric);
			if (contains) {
				break;
			}
		}
		return contains;
	}

	@EnableAutoConfiguration
	public static class BinderExporterApplication {

	}

}
