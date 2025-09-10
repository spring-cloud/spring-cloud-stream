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

package org.springframework.cloud.stream.binder;

/**
 * The binder customization strategy.
 * <p>
 * When customization beans are present in an application that uses a single binder,
 * those beans are detected by the binder. However, this is not the case in a multi-binder
 * scenario, since various binders live in different application contexts. This
 * customizer enables the application to properly apply customizations in all the
 * binders. By providing an implementation of this interface, the binders, although
 * reside in different application contexts, will receive the customization.
 * Spring Cloud Stream ensures that the customizations take place before the binders are accessed.
 * <p>
 * In the case of a single binder, the use of this customizer is redundant.
 *
 * @author Soby Chacko
 * @author Artme Bilan
 * @since 3.1.0
 */
public interface BinderCustomizer {

	/**
	 * The user must check for the binder type and then apply the necessary customizations.
	 * @param binder to be customized
	 * @param binderName binder name to distinguish between multiple instances of the same binder type
	 */
	void customize(Binder<?, ? extends ConsumerProperties, ? extends ProducerProperties> binder, String binderName);

}
