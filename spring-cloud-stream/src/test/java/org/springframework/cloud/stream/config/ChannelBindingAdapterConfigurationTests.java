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

package org.springframework.cloud.stream.config;

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
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.adapter.ChannelBinding;
import org.springframework.cloud.stream.adapter.ChannelBindingAdapter;
import org.springframework.cloud.stream.adapter.OutputChannelBinding;
import org.springframework.cloud.stream.binder.local.LocalMessageChannelBinder;
import org.springframework.cloud.stream.config.ChannelBindingAdapterConfigurationTests.Empty;
import org.springframework.cloud.stream.utils.MessageChannelBeanDefinitionRegistryUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
	private ChannelBindingProperties module;

	@Before
	public void init() {
	}

	@Test
	public void oneOutput() throws Exception {
		MessageChannelBeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", context);
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata().getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("output", channels.iterator().next().getRemoteName());
		assertEquals("tap:output", channels.iterator().next().getTapChannelName());
	}

	private void refresh() {
		Collection<OutputChannelBinding> channels = this.configuration.getOutputChannels();
		for (OutputChannelBinding channel : channels) {
			channel.setTapped(true);
		}
		this.adapter.stop();
		this.adapter.setOutputChannels(channels);
		this.adapter.start();
	}

	@Test
	public void twoOutputs() throws Exception {
		MessageChannelBeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", context);
		MessageChannelBeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("foo", context);
		module.getBindings().put("output", "group.0");
		module.getBindings().put("foo", "topic:group.0");
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata().getOutputChannels();
		List<String> remoteBindingNames = getRemoteBindingNames(channels);
		assertEquals(2, channels.size());
		assertTrue(remoteBindingNames.contains("group.0"));
		assertTrue(remoteBindingNames.contains("topic:group.0"));
	}

	private List<String> getRemoteBindingNames(Collection<? extends ChannelBinding> channels) {
		List<String> list = new ArrayList<String>();
		for (ChannelBinding binding : channels) {
			list.add(binding.getRemoteName());
		}
		return list;
	}

	@Test
	public void overrideNaturalOutputChannelName() throws Exception {
		MessageChannelBeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", context);
		module.getBindings().put("output", "group.0");
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata().getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("group.0", channels.iterator().next().getRemoteName());
		assertEquals("tap:group.0", channels.iterator().next().getTapChannelName());
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueueWithTopic() throws Exception {
		MessageChannelBeanDefinitionRegistryUtils.registerOutputChannelBeanDefinition("output", context);
		module.getBindings().put("output", "topic:group.0");
		refresh();
		Collection<OutputChannelBinding> channels = this.adapter.getChannelsMetadata().getOutputChannels();
		assertEquals(1, channels.size());
		assertEquals("topic:group.0", channels.iterator().next().getRemoteName());
		// is this correct?
		assertEquals("tap:group.0", channels.iterator().next().getTapChannelName());
	}

	@Configuration
	@Import({ChannelBindingAdapterConfiguration.class, PropertyPlaceholderAutoConfiguration.class})
	protected static class Empty {
		@Bean
		public LocalMessageChannelBinder binder() {
			return new LocalMessageChannelBinder();
		}
	}

}
