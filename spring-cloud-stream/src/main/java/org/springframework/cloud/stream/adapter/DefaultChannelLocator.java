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

import org.springframework.cloud.stream.config.ChannelBindingProperties;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 */
public class DefaultChannelLocator implements ChannelLocator {

	private ChannelBindingProperties module;

	public DefaultChannelLocator(ChannelBindingProperties module) {
		this.module = module;
	}

	@Override
	public String locate(String name) {
		return module.getBindingPath(name);
	}

	@Override
	public String tap(String name) {
		return this.module.getTapChannelName(getPlainBinding(name));
	}

	private String getPlainBinding(String name) {
		// remove prefixes such as 'tap:','topic:','queue:'
		if (name.contains(":")) {
			name = name.substring(name.indexOf(":") + 1);
		}
		return name;
	}

}
