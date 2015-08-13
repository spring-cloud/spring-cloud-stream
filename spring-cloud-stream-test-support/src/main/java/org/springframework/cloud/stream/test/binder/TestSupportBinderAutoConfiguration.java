/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * Installs the {@link TestSupportBinder} and exposes {@link MessageCollector} to be injected in tests.
 *
 * Note that this auto-configuration has higher priority than regular binders, so adding
 * this on the classpath in test scope is sufficient to have support kick in.
 *
 * @author Eric Bottard
 */
@Configuration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
public class TestSupportBinderAutoConfiguration {

	@Bean
	public MessageCollector messageCollector() {
		return new MessageCollector();
	}

	@Bean
	public Binder testSupportBinder() {
		return new TestSupportBinder(messageCollector());
	}

}
