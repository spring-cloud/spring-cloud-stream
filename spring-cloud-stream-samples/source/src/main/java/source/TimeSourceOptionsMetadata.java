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

package source;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.xd.module.options.validation.DateFormat;

/**
 * Describes options to the {@code time} source module.
 *
 * @author Eric Bottard
 * @author Gary Russell
 */
@ConfigurationProperties(prefix = "module.")
public class TimeSourceOptionsMetadata {

	/**
	 * 	how to render the current time, using SimpleDateFormat
	 */
	private String format = "yyyy-MM-dd HH:mm:ss";

	/**
	 * time delay between messages, expressed in TimeUnits (seconds by default)
	 */
	private int fixedDelay = 1;

	/**
	 * an initial delay when using a fixed delay trigger, expressed in TimeUnits (seconds by default)
	 */
	private int initialDelay = 0;

	/**
	 * the time unit for the fixed and initial delays
	 */
	private TimeUnit timeUnit = TimeUnit.SECONDS;

	/**
	 * the maximum messages per poll; -1 for unlimited
	 */
	long maxMessages = 1;

	public long getMaxMessages() {
		return this.maxMessages;
	}

	public void setMaxMessages(long maxMessages) {
		this.maxMessages = maxMessages;
	}

	@Min(0)
	public int getInitialDelay() {
		return this.initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

	public TimeUnit getTimeUnit() {
		return this.timeUnit;
	}

	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
	}

	@DateFormat
	public String getFormat() {
		return this.format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public int getFixedDelay() {
		return this.fixedDelay;
	}

	public void setFixedDelay(int fixedDelay) {
		this.fixedDelay = fixedDelay;
	}


}
