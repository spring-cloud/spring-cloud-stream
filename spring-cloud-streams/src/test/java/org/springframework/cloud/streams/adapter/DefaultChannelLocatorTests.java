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
package org.springframework.cloud.streams.adapter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.cloud.streams.adapter.DefaultChannelLocator;
import org.springframework.cloud.streams.config.MessageBusProperties;

/**
 * @author Dave Syer
 *
 */
public class DefaultChannelLocatorTests {

	private MessageBusProperties module = new MessageBusProperties();

	private DefaultChannelLocator locator = new DefaultChannelLocator(this.module);

	@Test
	public void oneOutput() throws Exception {
		assertEquals("group.0", this.locator.locate("output"));
	}

	@Test
	public void oneOutputTopic() throws Exception {
		assertEquals("topic:group.0", this.locator.locate("output.topic:"));
	}

	@Test
	public void outputWithNamedTopic() throws Exception {
		assertEquals("topic:foo.group.0", this.locator.locate("output.topic:foo"));
	}

	@Test
	public void outputWithNamedQueue() throws Exception {
		assertEquals("foo.group.0", this.locator.locate("output.queue:foo"));
	}

	@Test
	public void overrideNaturalOutputChannelName() throws Exception {
		this.module.setOutputChannelName("bar");
		assertEquals("foo.bar", this.locator.locate("output.queue:foo"));
	}

	@Test
	public void noQueueQualifier() throws Exception {
		assertEquals("foo.group.0", this.locator.locate("output.foo"));
	}

	@Test
	public void underscoreSeparatorForChannelName() throws Exception {
		assertEquals("foo.group.0", this.locator.locate("output_foo"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueue() throws Exception {
		this.module.setOutputChannelName("queue:bar");
		assertEquals("foo.bar", this.locator.locate("output.foo"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedQueueWithTopic() throws Exception {
		this.module.setOutputChannelName("queue:bar");
		assertEquals("topic:foo.bar", this.locator.locate("output.topic:foo"));
	}

	@Test
	public void overrideNaturalOutputChannelNamedTopic() throws Exception {
		this.module.setOutputChannelName("topic:bar");
		assertEquals("foo.bar", this.locator.locate("output.queue:foo"));
	}

}
