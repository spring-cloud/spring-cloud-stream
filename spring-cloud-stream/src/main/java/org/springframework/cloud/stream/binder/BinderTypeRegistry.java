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

package org.springframework.cloud.stream.binder;

import java.util.Map;

/**
 * A registry of {@link BinderType}s, indexed by name. A {@link BinderTypeRegistry} bean
 * is created automatically based on information found in the
 * {@literal META-INF/spring.binders} files provided by binder implementors. This can be
 * overridden by registering a {@link BinderTypeRegistry} bean in the context.
 *
 * @author Marius Bogoevici
 */
public interface BinderTypeRegistry {

	BinderType get(String name);

	Map<String, BinderType> getAll();

}
