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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Interface for customizing {@link StoreQueryParameters}.
 *
 * There are instances, in which an application wants to customize the internal
 * {@link StoreQueryParameters} object created by {@link InteractiveQueryService}
 * in {@link InteractiveQueryService#getQueryableStore(String, QueryableStoreType)} method.
 * Applications can provide an implementation for this customizer as a bean
 * and {@link InteractiveQueryService} will be provisioned with that customizer.
 *
 * @param <T> state store generic type
 *
 * @author Soby Chacko
 * @since 4.0.1
 */
@FunctionalInterface
public interface StoreQueryParametersCustomizer<T> {

	/**
	 * Customizing the internal {@link StoreQueryParameters} used by the {@link InteractiveQueryService}.
	 *
	 * @param storeQueryParameters Original {@link StoreQueryParameters} to customize
	 * @return the customized {@link StoreQueryParameters}
	 */
	StoreQueryParameters<T> customize(StoreQueryParameters<T> storeQueryParameters);
}
