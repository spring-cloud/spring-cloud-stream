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

package org.springframework.cloud.stream.binder.kstream.config;

import org.apache.kafka.streams.kstream.TimeWindows;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Soby Chacko
 */
@Configuration
@EnableConfigurationProperties(KStreamApplicationSupportProperties.class)
public class KStreamApplicationSupportAutoConfiguration {

	@Bean
	@ConditionalOnProperty("spring.cloud.stream.kstream.timeWindow.length")
	public TimeWindows configuredTimeWindow(KStreamApplicationSupportProperties processorProperties) {
		return processorProperties.getTimeWindow().getAdvanceBy() > 0
				? TimeWindows.of(processorProperties.getTimeWindow().getLength()).advanceBy(processorProperties.getTimeWindow().getAdvanceBy())
				: TimeWindows.of(processorProperties.getTimeWindow().getLength());
	}
}
