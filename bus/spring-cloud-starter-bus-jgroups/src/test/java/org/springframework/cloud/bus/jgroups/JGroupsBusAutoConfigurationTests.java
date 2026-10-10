/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.bus.jgroups;

import java.util.function.Consumer;

import org.jgroups.JChannel;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.bus.BusBridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class JGroupsBusAutoConfigurationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withPropertyValues("spring.cloud.bus.enabled=true")
		.withUserConfiguration(JGroupsBusAutoConfiguration.class)
		.withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
		.withBean(Consumer.class, () -> mock(Consumer.class))
		.withBean(JGroupsChannelFactory.class, () -> {
			JGroupsChannelFactory factory = mock(JGroupsChannelFactory.class);
			JChannel channel = mock(JChannel.class);
			try {
				doReturn(channel).when(factory).create();
			}
			catch (Exception ex) {
				throw new IllegalStateException(ex);
			}
			return factory;
		});

	@Test
	void createsJGroupsBusBridge() {
		this.contextRunner.run(context -> assertThat(context).hasSingleBean(JGroupsBusBridge.class));
	}

	@Test
	void backsOffWhenBusBridgeAlreadyExists() {
		this.contextRunner.withBean(BusBridge.class, () -> mock(BusBridge.class))
			.run(context -> assertThat(context).doesNotHaveBean(JGroupsBusBridge.class));
	}

}
