/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.xd.dirt.modules.metadata;

import org.springframework.xd.module.options.mixins.MaxMessagesDefaultOneMixin;
import org.springframework.xd.module.options.mixins.PeriodicTriggerMixin;
import org.springframework.xd.module.options.spi.Mixin;
import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.validation.DateFormat;

/**
 * Describes options to the {@code time} source module.
 *
 * @author Eric Bottard
 * @author Gary Russell
 */
@Mixin({ PeriodicTriggerMixin.class, MaxMessagesDefaultOneMixin.class })
public class TimeSourceOptionsMetadata {

	private String format = "yyyy-MM-dd HH:mm:ss";

	private int fixedDelay = 1;


	@DateFormat
	public String getFormat() {
		return format;
	}

	@ModuleOption("how to render the current time, using SimpleDateFormat")
	public void setFormat(String format) {
		this.format = format;
	}


	public int getFixedDelay() {
		return fixedDelay;
	}


	@ModuleOption("time delay between messages, expressed in TimeUnits (seconds by default)")
	public void setFixedDelay(int fixedDelay) {
		this.fixedDelay = fixedDelay;
	}


}
