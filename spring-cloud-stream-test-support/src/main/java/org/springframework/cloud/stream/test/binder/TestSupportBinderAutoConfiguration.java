/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.messaging.MessageChannel;

/**
 * Installs the TestSupportBinder} and exposes
 * MessageCollectorImplto be injected in tests.
 *
 * Note that this auto-configuration has higher priority than regular binder
 * configuration, so adding this on the classpath in test scope is sufficient to have
 * support kick in and replace all binders with the test binder.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 */
@Configuration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
@Import(TestSupportBinderConfiguration.class)
@AutoConfigureBefore(BindingServiceConfiguration.class)
public class TestSupportBinderAutoConfiguration {

	@Bean
	@SuppressWarnings("unchecked")
	public BinderFactory testSupportBinderFactory(final Binder<MessageChannel, ?, ?> binder) {
		return new BinderFactory() {
			@Override
			public <T> Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties> getBinder(
					String configurationName, Class<? extends T> bindableType) {
				return (Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties>) binder;
			}
		};
	}

}
