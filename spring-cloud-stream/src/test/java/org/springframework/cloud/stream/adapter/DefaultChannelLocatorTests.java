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
package org.springframework.cloud.stream.adapter;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;
import org.springframework.cloud.stream.adapter.DefaultChannelLocator;
import org.springframework.cloud.stream.config.ChannelBindingProperties;

/**
 * @author Dave Syer
 *
 */
public class DefaultChannelLocatorTests {

	private ChannelBindingProperties module = new ChannelBindingProperties();

	private DefaultChannelLocator locator = new DefaultChannelLocator(this.module);

	@Test
	public void oneOutput() throws Exception {
		assertEquals("output", this.locator.locate("output"));
	}

	@Test
	public void oneOutputWithShortcutPath() throws Exception {
		module.getBindings().put("outputWithShortcutPath","group.0.shortcut");
		assertEquals("group.0.shortcut", this.locator.locate("outputWithShortcutPath"));
	}

	@Test
	public void oneOutputWithFullPath() throws Exception {
		module.getBindings().put("outputWithFullPath", Collections.singletonMap(ChannelBindingProperties.PATH,"group.0.full"));
		assertEquals("group.0.full", this.locator.locate("outputWithFullPath"));
	}


	@Test
	public void oneOutputTopic() throws Exception {
		module.getBindings().put("outputWithTopic", Collections.singletonMap(ChannelBindingProperties.PATH,"topic:group.0"));
		assertEquals("topic:group.0", this.locator.locate("outputWithTopic"));
	}

}
