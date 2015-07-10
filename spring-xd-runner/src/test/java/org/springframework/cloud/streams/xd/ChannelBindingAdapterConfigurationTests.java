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

package org.springframework.cloud.streams.xd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.streams.adapter.ChannelBinding;
import org.springframework.cloud.streams.adapter.ChannelBindingAdapter;
import org.springframework.cloud.streams.adapter.InputChannelBinding;
import org.springframework.cloud.streams.adapter.OutputChannelBinding;
import org.springframework.cloud.streams.config.ChannelBindingAdapterConfiguration;
import org.springframework.cloud.streams.utils.BeanDefinitionRegistryUtils;
import org.springframework.cloud.streams.xd.ChannelBindingAdapterConfigurationTests.Empty;
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
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Empty.class)
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class ChannelBindingAdapterConfigurationTests {

	@Autowired
	private DefaultListableBeanFactory context;

	@Autowired
	private ChannelBindingAdapter adapter;

	@Autowired
	private ChannelBindingAdapterConfiguration configuration;

	@Autowired
	private ModuleProperties module;

	@Before
	public void init() {
	}

	@Test
	public void oneOutput() throws Exception {
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", context);
		this.context.registerSingleton("output", new DirectChannel());
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("group.0", channels.iterator().next().getRemoteName());
		assertEquals("tap:stream:group.module.0", channels.iterator().next()
				.getTapChannelName());
	}

	@Test
	public void oneInput() throws Exception {
		BeanDefinitionRegistryUtils.registerInputChannelBeanDefinition("input",context);
		refresh();
		Collection<InputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getInputChannels();
		assertEquals(1, channels.size());
		assertEquals("group.0", channels.iterator().next().getRemoteName());
	}

	private void refresh() {
		this.configuration.refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		for (OutputChannelBinding channel : channels) {
			channel.setTapped(true);
		}
		this.adapter.setOutputChannels(channels);
		this.adapter.start();
	}

	@Test
	public void oneOutputTopic() throws Exception {
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output.topic:", this.context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("topic:group.0", channels.iterator().next().getRemoteName());
		assertEquals("tap:stream:group.module.0", channels.iterator().next()
				.getTapChannelName());
	}

	@Test
	public void oneOutputOverrideName() throws Exception {
		this.module.setGroup("mine");
		this.module.setName("foo");
		this.module.setIndex(2);
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output.topic:", this.context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("topic:mine.2", channels.iterator().next().getRemoteName());
		assertEquals("tap:stream:mine.foo.2", channels.iterator().next()
				.getTapChannelName());
	}

	@Test
	public void oneInputTopic() throws Exception {
		BeanDefinitionRegistryUtils.registerInputChannelBeanDefinition("input.topic:", this.context);
		refresh();
		Collection<InputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getInputChannels();
		assertEquals(1, channels.size());
		assertEquals("topic:group.0", channels.iterator().next().getRemoteName());
	}

	@Test
	public void oneInputOverrideName() throws Exception {
		this.module.setGroup("mine");
		this.module.setIndex(2);
		BeanDefinitionRegistryUtils.registerInputChannelBeanDefinition("input", this.context);
		refresh();
		Collection<InputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getInputChannels();
		assertEquals(1, channels.size());
		assertEquals("mine.1", channels.iterator().next().getRemoteName());
	}

	@Test
	public void twoOutputsWithQueue() throws Exception {
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", this.context);
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output.queue:foo", this.context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		List<String> names = getChannelNames(channels);
		assertEquals(2, channels.size());
		assertTrue(names.contains("group.0"));
		assertTrue(names.contains("foo.group.0"));
	}

	private List<String> getChannelNames(Collection<? extends ChannelBinding> channels) {
		List<String> list = new ArrayList<String>();
		for (ChannelBinding binding : channels) {
			list.add(binding.getRemoteName());
		}
		return list;
	}

	@Test
	public void overrideNaturalOutputChannelName() throws Exception {
		this.module.setOutputChannelName("bar");
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output.queue:foo", this.context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("foo.bar", channels.iterator().next().getRemoteName());
		// TODO: fix this. What should it be?
		assertEquals("tap:stream:foo.bar.module.0", channels.iterator().next()
				.getTapChannelName());
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueueWithTopic() throws Exception {
		this.module.setOutputChannelName("queue:bar");
		BeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output.topic:foo", this.context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata()
				.getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("topic:foo.bar", channels.iterator().next().getRemoteName());
		assertEquals("tap:stream:foo.bar.module.0", channels.iterator().next()
				.getTapChannelName());
	}

	@Configuration
	@Import(ChannelBindingAdapterConfiguration.class)
	protected static class Empty {
		@Bean
		public LocalMessageBus messageBus() {
			return new LocalMessageBus();
		}
	}

}
