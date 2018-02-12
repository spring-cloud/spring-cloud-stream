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

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@link ConfigurationProperties} that can be used by end user Kafka Stream applications. This class provides
 * convenient ways to access the commonly used kafka stream properties from the user application. For example, windowing
 * operations are common use cases in stream processing and one can provide window specific properties at runtime and use
 * those properties in the applications using this class.
 *
 * @author Soby Chacko
 */
@ConfigurationProperties("spring.cloud.stream.kafka.streams")
public class KafkaStreamsApplicationSupportProperties {

	private TimeWindow timeWindow;

	public TimeWindow getTimeWindow() {
		return timeWindow;
	}

	public void setTimeWindow(TimeWindow timeWindow) {
		this.timeWindow = timeWindow;
	}

	public static class TimeWindow {

		private int length;

		private int advanceBy;

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		}

		public int getAdvanceBy() {
			return advanceBy;
		}

		public void setAdvanceBy(int advanceBy) {
			this.advanceBy = advanceBy;
		}
	}
}
