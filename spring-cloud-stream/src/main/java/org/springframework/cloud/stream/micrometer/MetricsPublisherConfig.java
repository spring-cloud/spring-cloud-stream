/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.micrometer;

import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * @author Oleg Zhurakousky
 * @since 2.0
 *
 */
class MetricsPublisherConfig implements StepRegistryConfig {

	private final ApplicationMetricsProperties applicationMetricsProperties;

	MetricsPublisherConfig(ApplicationMetricsProperties applicationMetricsProperties) {
		this.applicationMetricsProperties = applicationMetricsProperties;
	}

	@Override
	public String prefix() {
		return ApplicationMetricsProperties.PREFIX;
	}

	@Override
	public String get(String key) {
		String value = null;
		if (key.equals(this.prefix() + ".step")) {
			value = this.applicationMetricsProperties.getScheduleInterval().toString();
		}
		return value;
	}

}
