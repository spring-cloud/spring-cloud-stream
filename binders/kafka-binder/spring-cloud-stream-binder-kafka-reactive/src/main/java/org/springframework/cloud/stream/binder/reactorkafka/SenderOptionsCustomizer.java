/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import java.util.function.BiFunction;

import reactor.kafka.sender.SenderOptions;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * Customizer for {@link SenderOptions}; the first parameter contains the binder name; the
 * second is the {@link SenderOptions} to customize. Return the customized
 * {@link SenderOptions}. Applied in {@link Order} if multiple customizers are found.
 *
 * @author Gary Russell
 * @since 4.0.2
 *
 */
public interface SenderOptionsCustomizer
		extends BiFunction<String, SenderOptions<Object, Object>, SenderOptions<Object, Object>>, Ordered {

	@Override
	default int getOrder() {
		return 0;
	}

}
