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

import org.springframework.cloud.streams.config.ChannelBindingProperties;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.BusUtils;

/**
 * @author Dave Syer
 *
 */
public class ModuleProperties extends ChannelBindingProperties {

	private String group = "group";
	private String name = "module";
	private int index = 0;
	private String type = "processor";

	private Tap tap;

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getGroup() {
		return this.group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIndex() {
		return this.index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	@Override
	public String getOutputChannelName() {
		String name = super.getOutputChannelName();
		if (ChannelBindingProperties.DEFAULT_CHANNEL_NAME.equals(name)) {
			return BusUtils.constructPipeName(this.group, this.index);
		}
		return name;
	}

	@Override
	public String getInputChannelName() {
		if (isTap()) {
			return String.format("%s.%s.%s",
					BusUtils.constructTapPrefix(this.tap.getGroup()), this.tap.getName(),
					this.tap.getIndex());
		}
		String name = super.getInputChannelName();
		if (ChannelBindingProperties.DEFAULT_CHANNEL_NAME.equals(name)) {
			return BusUtils.constructPipeName(this.group, this.index > 0 ? this.index - 1
					: this.index);
		}
		return name;
	}

	@Override
	public String getTapChannelName() {
		return getTapChannelName(this.group);
	}

	@Override
	public String getTapChannelName(String prefix) {
		Assert.isTrue(!this.type.equals("job"), "Job module type not supported.");
		// for Stream return channel name with indexed elements
		return String.format("%s.%s.%s", BusUtils.constructTapPrefix(prefix), this.name,
				this.index);
	}

	public Tap getTap() {
		return this.tap;
	}

	public void setTap(Tap tap) {
		this.tap = tap;
	}

	private boolean isTap() {
		if (this.tap != null) {
			Assert.state(this.tap.getName() != null, "Tap name not provided");
			Assert.state(!this.tap.getGroup().equals(this.group),
					"Tap group cannot be the same as module group");
		}
		return this.tap != null;
	}

	public static class Tap {

		private String group = "group";

		private String name;

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		private int index = 0;

		public String getGroup() {
			return this.group;
		}

		public void setGroup(String group) {
			this.group = group;
		}

		public int getIndex() {
			return this.index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

	}
}
