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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DefaultBinderFactory}.
 *
 * @author Chris Bono
 */
class DefaultBinderFactoryTests {

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

}
