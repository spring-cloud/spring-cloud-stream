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
package org.springframework.bus.runner.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.bus.runner.adapter.OutputChannelSpec;
import org.springframework.bus.runner.config.MessageBusAdapterConfigurationTests.Empty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.xd.dirt.integration.bus.local.LocalMessageBus;

/**
 * @author Dave Syer
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Empty.class)
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class MessageBusAdapterConfigurationTests {

	@Autowired
	private DefaultListableBeanFactory context;

	@Autowired
	private MessageBusAdapterConfiguration configuration;

	@Autowired
	private MessageBusProperties module;

	@Test
	public void oneOutput() throws Exception {
		context.registerSingleton("output", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(1, channels.size());
		assertTrue(channels.containsKey("group.0"));
	}

	@Test
	public void twoOutputsWithTopic() throws Exception {
		context.registerSingleton("output", new DirectChannel());
		context.registerSingleton("output.topic:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(2, channels.size());
		assertTrue(channels.containsKey("group.0"));
		assertTrue(channels.containsKey("topic:foo.group.0"));
	}

	@Test
	public void twoOutputsWithQueue() throws Exception {
		context.registerSingleton("output", new DirectChannel());
		context.registerSingleton("output.queue:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(2, channels.size());
		assertTrue(channels.containsKey("group.0"));
		assertTrue(channels.containsKey("foo.group.0"));
	}

	@Test
	public void overrideNaturalOutputChannelName() throws Exception {
		module.setOutputChannelName("bar");
		context.registerSingleton("output.queue:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(1, channels.size());
		assertTrue(channels.containsKey("foo.bar"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueue() throws Exception {
		module.setOutputChannelName("queue:bar");
		context.registerSingleton("output.queue:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(1, channels.size());
		assertTrue("Wrong key: " + channels.keySet(), channels.containsKey("foo.bar"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueueWithTopic() throws Exception {
		module.setOutputChannelName("queue:bar");
		context.registerSingleton("output.topic:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(1, channels.size());
		assertTrue("Wrong key: " + channels.keySet(), channels.containsKey("topic:foo.bar"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedTopic() throws Exception {
		module.setOutputChannelName("topic:bar");
		context.registerSingleton("output.queue:foo", new DirectChannel());
		Map<String, OutputChannelSpec> channels = configuration.getOutputChannels();
		assertEquals(1, channels.size());
		assertTrue("Wrong key: " + channels.keySet(), channels.containsKey("foo.bar"));
	}

	@Configuration
	@Import(MessageBusAdapterConfiguration.class)
	protected static class Empty {
		@Bean
		public LocalMessageBus messageBus() {
			return new LocalMessageBus();
		}
	}

}
