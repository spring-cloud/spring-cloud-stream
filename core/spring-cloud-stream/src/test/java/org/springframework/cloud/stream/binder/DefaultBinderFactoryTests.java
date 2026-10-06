/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DefaultBinderFactory}.
 *
 * @author Chris Bono
 * @author Sharang Gupta
 */
class DefaultBinderFactoryTests {

	private static final String PLAIN_LIFECYCLE_BEAN = "plainLifecycleBean";

	private static final String SMART_LIFECYCLE_BEAN = "smartLifecycleBean";

	@Test
	void updateBinderConfigurations() {
		Map<String, BinderConfiguration> binderConfigs = new HashMap<>();
		binderConfigs.put("foo", mock(BinderConfiguration.class));
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(binderConfigs, null, null);

		Map<String, BinderConfiguration> newBinderConfigs = new HashMap<>();
		newBinderConfigs.put("bar", mock(BinderConfiguration.class));
		binderFactory.updateBinderConfigurations(newBinderConfigs);

		assertThat(binderFactory.getBinderConfigurations()).containsExactlyInAnyOrderEntriesOf(newBinderConfigs);
	}

	@Test
	void startDoesNotStartPlainLifecycleBeansOfBinderContexts() {
		DefaultBinderFactory binderFactory = createBinderFactoryWithLifecycleBeans();
		ConfigurableApplicationContext binderContext = createBinderContext(binderFactory);

		binderFactory.start();

		assertThat(lifecycleBean(binderContext, PLAIN_LIFECYCLE_BEAN).isRunning()).isFalse();
		assertThat(lifecycleBean(binderContext, SMART_LIFECYCLE_BEAN).isRunning()).isTrue();
		binderFactory.destroy();
	}

	@Test
	void startAfterStopRestartsBinderContexts() {
		DefaultBinderFactory binderFactory = createBinderFactoryWithLifecycleBeans();
		ConfigurableApplicationContext binderContext = createBinderContext(binderFactory);
		binderFactory.start();

		binderFactory.stop();

		assertThat(binderFactory.isRunning()).isFalse();
		assertThat(binderContext.isRunning()).isFalse();
		assertThat(lifecycleBean(binderContext, SMART_LIFECYCLE_BEAN).isRunning()).isFalse();

		binderFactory.start();

		assertThat(binderFactory.isRunning()).isTrue();
		assertThat(binderContext.isRunning()).isTrue();
		assertThat(lifecycleBean(binderContext, SMART_LIFECYCLE_BEAN).isRunning()).isTrue();
		binderFactory.destroy();
	}

	private static DefaultBinderFactory createBinderFactoryWithLifecycleBeans() {
		BinderType binderType = new BinderType("mock",
				new Class[] { MockBinderConfiguration.class, LifecycleBeansBinderConfiguration.class });
		BinderTypeRegistry binderTypeRegistry = new DefaultBinderTypeRegistry(
				Collections.singletonMap("mock", binderType));
		BinderConfiguration binderConfiguration = new BinderConfiguration("mock", new HashMap<>(), true, true);
		return new DefaultBinderFactory(Collections.singletonMap("mock", binderConfiguration), binderTypeRegistry,
				null);
	}

	private static ConfigurableApplicationContext createBinderContext(DefaultBinderFactory binderFactory) {
		AtomicReference<ConfigurableApplicationContext> binderContext = new AtomicReference<>();
		binderFactory
			.setListeners(List.of((configurationName, initializedContext) -> binderContext.set(initializedContext)));
		binderFactory.getBinder(null, MessageChannel.class);
		return binderContext.get();
	}

	private static Lifecycle lifecycleBean(ConfigurableApplicationContext binderContext, String beanName) {
		return binderContext.getBean(beanName, Lifecycle.class);
	}

	@Configuration
	static class LifecycleBeansBinderConfiguration {

		@Bean(PLAIN_LIFECYCLE_BEAN)
		Lifecycle plainLifecycleBean() {
			return new RunningStateLifecycle();
		}

		@Bean(SMART_LIFECYCLE_BEAN)
		SmartLifecycle smartLifecycleBean() {
			return new RunningStateSmartLifecycle();
		}

	}

	static class RunningStateLifecycle implements Lifecycle {

		private volatile boolean running;

		@Override
		public void start() {
			this.running = true;
		}

		@Override
		public void stop() {
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

	}

	static class RunningStateSmartLifecycle extends RunningStateLifecycle implements SmartLifecycle {

	}

}
