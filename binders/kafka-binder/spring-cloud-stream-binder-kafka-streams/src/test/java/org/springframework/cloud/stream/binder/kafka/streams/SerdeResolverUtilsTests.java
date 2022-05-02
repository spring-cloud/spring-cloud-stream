/*
 * Copyright 2022-2022 the original author or authors.
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

import java.util.Date;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SerdeResolverUtils}.
 *
 * @author Chris Bono
 */
class SerdeResolverUtilsTests {

	/**
	 * Verify that {@link SerdeResolverUtils#findMatchingSerdes} returns the proper serdes
	 * in the proper order for the following grid:
	 * <p><br>
	 * <pre>{@code
	 * ------------------------------------------------------------------
	 * KStream type       | Serde type
	 * ------------------------------------------------------------------
	 *                    | GE<Date> | GE<? extends Date> | GE<?> | GE
	 * ------------------------------------------------------------------
	 * GE<Date>           | Y        | Y                  | Y     | Y
	 * GE<? extends Date> | N        | Y                  | Y     | N
	 * GE<?>              | N        | N                  | Y     | N
	 * GE                 | N        | N                  | N     | N
	 * ------------------------------------------------------------------
	 * }</pre>
	 */
	@Test
	void findMatchingSerdesForSimpleGenericType() {

		ResolvableType geDate = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Date>>() { });
		ResolvableType geBounded = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Date>>() { });
		ResolvableType geWildcard = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<?>>() { });
		ResolvableType geRaw = ResolvableType.forRawClass(GenericEvent.class);

		ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(SerdeResolverTestApp.class);

		contextRunner.run((context) -> {

			assertThat(SerdeResolverUtils.findMatchingSerdes(context, geDate))
				.extracting("name")
				.containsExactly("genericEventDateSerde", "genericEventDateBoundedSerde", "genericEventWildcardSerde", "genericEventRawSerde");

			assertThat(SerdeResolverUtils.findMatchingSerdes(context, geBounded))
				.extracting("name")
				.containsExactly("genericEventDateBoundedSerde", "genericEventWildcardSerde");

			assertThat(SerdeResolverUtils.findMatchingSerdes(context, geWildcard))
				.extracting("name")
				.containsExactly("genericEventWildcardSerde");

			// Because GenericEvent is a parameterized type, Serde<GenericEvent> resolves to Serde<GenericEvent<?>>
			// which is not assignable from GenericEvent
			assertThat(SerdeResolverUtils.findMatchingSerdes(context, geRaw))
				.extracting("name")
				.isEmpty();
		});
	}

	static class GenericEventSerde<T> implements Serde<GenericEvent<? extends T>> {
		private String name;

		GenericEventSerde(String name) {
			this.name = name;
		}

		String getName() {
			return name;
		}

		@Override
		public Serializer<GenericEvent<? extends T>> serializer() {
			return null;
		}

		@Override
		public Deserializer<GenericEvent<? extends T>> deserializer() {
			return null;
		}
	}

	static class GenericEvent<T> { }

	@EnableAutoConfiguration
	static class SerdeResolverTestApp {

		@Bean
		public Serde<GenericEvent<Date>> genericEventDateSerde() {
			return new GenericEventSerde("genericEventDateSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Date>> genericEventDateBoundedSerde() {
			return new GenericEventSerde("genericEventDateBoundedSerde");
		}

		@Bean
		public Serde<GenericEvent<String>> genericEventStringSerde() {
			return new GenericEventSerde("genericEventStringSerde");
		}

		@Bean
		public Serde<GenericEvent<?>> genericEventWildcardSerde() {
			return new GenericEventSerde("genericEventWildcardSerde");
		}

		@Bean
		public Serde<GenericEvent> genericEventRawSerde() {
			return new GenericEventSerde("genericEventRawSerde");
		}

		@Bean
		public Serde<?> widlcardSerde() {
			return Serdes.Void();
		}
	}
}
