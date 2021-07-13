/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus;

import org.junit.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.endpoint.RefreshBusEndpoint;
import org.springframework.cloud.bus.event.RefreshListener;

import static org.assertj.core.api.Assertions.assertThat;

public class BusAutoConfigurationClassPathTests {

	@Test
	public void refreshListenerCreatedWithoutActuator() {
		new ApplicationContextRunner().withClassLoader(new FilteredClassLoader("org.springframework.boot.actuate"))
				.withConfiguration(AutoConfigurations.of(RefreshAutoConfiguration.class,
						PathServiceMatcherAutoConfiguration.class, BusRefreshAutoConfiguration.class))
				.run(context -> assertThat(context).hasSingleBean(RefreshListener.class)
						.doesNotHaveBean(RefreshBusEndpoint.class));
	}

}
