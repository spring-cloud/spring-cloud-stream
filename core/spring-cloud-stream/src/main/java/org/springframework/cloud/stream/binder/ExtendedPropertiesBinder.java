/*
 * Copyright 2016-present the original author or authors.
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
 * Extension of {@link Binder} that takes {@link ExtendedConsumerProperties} and
 * {@link ExtendedProducerProperties} as arguments. In addition to supporting binding
 * operations, it allows the binder to provide values for the additional properties it
 * expects on the bindings.
 *
 * @param <T> binder type
 * @param <C> consumer type
 * @param <P> producer type
 * @author Marius Bogoevici
 */
public interface ExtendedPropertiesBinder<T, C, P>
		extends Binder<T, ExtendedConsumerProperties<C>, ExtendedProducerProperties<P>>,
		ExtendedBindingProperties<C, P> {

}
