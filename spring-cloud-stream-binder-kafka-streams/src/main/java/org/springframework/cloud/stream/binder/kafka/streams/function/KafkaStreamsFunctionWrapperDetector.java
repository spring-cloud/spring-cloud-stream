/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.lang.reflect.Type;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.cloud.function.context.WrapperDetector;

/**
 * @author Soby Chacko
 * @since 2.2.0
 */
public class KafkaStreamsFunctionWrapperDetector implements WrapperDetector {
	@Override
	public boolean isWrapper(Type type) {
		if (type instanceof Class<?>) {
			Class<?> cls = (Class<?>) type;
			return KStream.class.isAssignableFrom(cls) ||
					KTable.class.isAssignableFrom(cls) ||
					GlobalKTable.class.isAssignableFrom(cls);
		}
		return false;
	}
}

