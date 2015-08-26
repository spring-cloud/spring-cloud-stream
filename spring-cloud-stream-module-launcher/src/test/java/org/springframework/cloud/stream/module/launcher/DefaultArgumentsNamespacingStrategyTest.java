/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.module.launcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

/**
 * Tests for DefaultArgumentsNamespacingStrategy.
 *
 * @author Eric Bottard
 */
public class DefaultArgumentsNamespacingStrategyTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testInverse() {
		DefaultArgumentsNamespacingStrategy strategy = new DefaultArgumentsNamespacingStrategy();
		String module = "org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT";

		Map<String, ?> qualified = strategy.qualify(module, Collections.singletonMap("foo", "bar"));
		MutablePropertySources sources = new MutablePropertySources();
		sources.addFirst(new MapPropertySource("props", (Map<String, Object>) qualified));
		assertThat(strategy.unqualify(module, sources), is(Collections.singletonMap("foo", "bar")));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testIsolation() {
		DefaultArgumentsNamespacingStrategy strategy = new DefaultArgumentsNamespacingStrategy();
		String module = "org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT";
		String otherModule = "org.springframework.cloud.stream.module:log-sink:1.0.0.BUILD-SNAPSHOT";

		Map<String, ?> qualified = strategy.qualify(module, Collections.singletonMap("foo", "bar"));
		MutablePropertySources sources = new MutablePropertySources();
		sources.addFirst(new MapPropertySource("props", (Map<String, Object>) qualified));
		assertThat(strategy.unqualify(otherModule, sources), is(Collections.<String, String>emptyMap()));
	}

}