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

package org.springframework.cloud.stream.utils;

import java.util.Collections;

import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * A simple configuration that creates mock
 * {@link org.springframework.cloud.stream.binder.Binder}s.
 * @author Marius Bogoevici
 */
@Configuration
public class MockBinderRegistryConfiguration {

	@Bean
	public BinderTypeRegistry binderTypeRegistry() {
		return new DefaultBinderTypeRegistry(
				Collections.singletonMap("mock", new BinderType("", new Class[] { MockBinderConfiguration.class })));
	}
}
