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
import org.junit.Assert;
import org.junit.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;

/**
 * @author Vinicius Carvalho
 */
public class BinderMetricsEmitterTests {

	@Test(expected = NoSuchBeanDefinitionException.class)
	public void checkDisabledConfiguration() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500"
		);
		try {
			applicationContext.getBean(Emitter.class);
		} catch (Exception e){
			throw e;
		}
		finally {
			applicationContext.close();
		}

	}

	@Test
	public void defaultIncludes() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500",
				"--spring.cloud.stream.bindings.streamMetrics.destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message message = collector.forChannel(emitterSource.metrics()).poll(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((String)message.getPayload(),ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean", applicationMetrics.getMetrics()));
		Assert.assertFalse(contains("mem",applicationMetrics.getMetrics()));
		Assert.assertEquals("application",applicationMetrics.getName());
		Assert.assertEquals(0,applicationMetrics.getInstanceIndex());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void customAppNameAndIndex() throws Exception{
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500",
				"--spring.application.name=foo",
				"--spring.cloud.stream.instanceIndex=1",
				"--spring.cloud.stream.bindings.streamMetrics.destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message message = collector.forChannel(emitterSource.metrics()).poll(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((String)message.getPayload(),ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean", applicationMetrics.getMetrics()));
		Assert.assertFalse(contains("mem",applicationMetrics.getMetrics()));
		Assert.assertEquals("foo",applicationMetrics.getName());
		Assert.assertEquals(1,applicationMetrics.getInstanceIndex());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void usingPrefix() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500",
				"--spring.cloud.stream.metrics.prefix=foo",
				"--spring.cloud.stream.instanceIndex=1",
				"--spring.cloud.stream.bindings.streamMetrics.destination=foo");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message message = collector.forChannel(emitterSource.metrics()).poll(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((String)message.getPayload(),ApplicationMetrics.class);
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean", applicationMetrics.getMetrics()));
		Assert.assertFalse(contains("mem",applicationMetrics.getMetrics()));
		Assert.assertEquals("foo.application",applicationMetrics.getName());
		Assert.assertEquals(1,applicationMetrics.getInstanceIndex());
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void includesExcludes() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500",
				"--spring.cloud.stream.bindings.streamMetrics.destination=foo",
				"--spring.cloud.stream.metrics.includes=mem**",
				"--spring.cloud.stream.metrics.excludes=integration**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message message = collector.forChannel(emitterSource.metrics()).poll(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((String)message.getPayload(),ApplicationMetrics.class);
		Assert.assertFalse(contains("integration.channel.errorChannel.errorRate.mean", applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("mem",applicationMetrics.getMetrics()));
		Assert.assertTrue(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	@Test
	public void includesExcludesWithProperties() throws Exception {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(BinderExporterApplication.class,
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.metrics.delay-millis=500",
				"--spring.cloud.stream.bindings.streamMetrics.destination=foo",
				"--spring.cloud.stream.metrics.includes=integration**",
				"--spring.cloud.stream.metrics.properties=java**");
		Emitter emitterSource = applicationContext.getBean(Emitter.class);
		MessageCollector collector = applicationContext.getBean(MessageCollector.class);
		Message message = collector.forChannel(emitterSource.metrics()).poll(1000, TimeUnit.MILLISECONDS);
		Assert.assertNotNull(message);
		ObjectMapper mapper = applicationContext.getBean(ObjectMapper.class);
		ApplicationMetrics applicationMetrics = mapper.readValue((String)message.getPayload(),ApplicationMetrics.class);
		Assert.assertFalse(contains("mem", applicationMetrics.getMetrics()));
		Assert.assertTrue(contains("integration.channel.errorChannel.errorRate.mean",applicationMetrics.getMetrics()));
		Assert.assertFalse(CollectionUtils.isEmpty(applicationMetrics.getProperties()));
		applicationContext.close();
	}

	private boolean contains(String metric, Collection<Metric> metrics){
		boolean contains = false;
		for(Metric entry : metrics){
			contains = entry.getName().equals(metric);
			if(contains){
				break;
			}
		}
		return contains;
	}

	@EnableAutoConfiguration
	public static class BinderExporterApplication {

	}
}
