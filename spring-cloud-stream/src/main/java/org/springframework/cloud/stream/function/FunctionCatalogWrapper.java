/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 **/
class FunctionCatalogWrapper {

	private final FunctionCatalog catalog;

	FunctionCatalogWrapper(FunctionCatalog catalog) {
		this.catalog = catalog;
	}

	<T> T lookup(Class<T> functionType, String name) {
		T function = catalog.lookup(functionType, name);
		Assert.notNull(function, functionType == null ?
			String.format("User provided Function '%s' cannot be located.", name) :
			String.format("User provided %s '%s' cannot be located.", functionType.getSimpleName(), name));
		return function;
	}

	<T> T lookup(String name) {
		return lookup(null, name);
	}

	<T> boolean contains(Class<T> functionType, String name) {
		return catalog.lookup(functionType, name) != null;
	}
}
