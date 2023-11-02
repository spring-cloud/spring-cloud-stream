/*
 * Copyright 2022-2023 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link SerdeResolverUtils}.
 *
 * @author Chris Bono
 */
@SuppressWarnings({ "rawtypes", "NewClassNamingConvention", "unchecked" })
class SerdeResolverUtilsTests {

	@Nested
	class ResolveForType {

		private ApplicationContextRunner contextRunner = new ApplicationContextRunner()
				.withPropertyValues("spring.cloud.function.ineligible-definitions: sendToDlqAndContinue");

		private Serde<?> fallback = mock(Serde.class);

		@Test
		void returnsFallbackSerdeForWildcard() {
			this.contextRunner
				.withConfiguration(AutoConfigurations.of(SerdeResolverSimpleTestApp.class))
				.run((context) -> {
					ResolvableType wildcardType = ResolvableType.forClass(Serde.class).getGeneric(0);
					assertThat(SerdeResolverUtils.resolveForType(context, wildcardType, fallback)).isSameAs(fallback);
				});
		}

		@Test
		void returnsSerdeBeanForMatchingType() {
			this.contextRunner
				.withConfiguration(AutoConfigurations.of(SerdeResolverSimpleTestApp.class))
				.run((context) -> {
					ResolvableType fooType = ResolvableType.forClass(Foo.class);
					assertThat(SerdeResolverUtils.resolveForType(context, fooType, fallback)).isInstanceOf(FooSerde.class);
				});
		}

		@Nested
		class NoMatchingSerdeBeans {

			@ParameterizedTest
			@MethodSource("kafkaStreamsBuiltInTypes")
			void returnsStandardSerdeForKafkaStreamsBuiltInType(Class<?> builtInType, Serde<?> expectedBuiltInSerde) {
				contextRunner.run((context) ->
					assertThat(SerdeResolverUtils.resolveForType(context, ResolvableType.forClass(builtInType), fallback))
						.isInstanceOf(expectedBuiltInSerde.getClass()));
			}

			static Stream<Arguments> kafkaStreamsBuiltInTypes() {
				return Stream.of(
					arguments(String.class, Serdes.String()),
					arguments(Short.class, Serdes.Short()),
					arguments(Integer.class, Serdes.Integer()),
					arguments(Long.class, Serdes.Long()),
					arguments(Float.class, Serdes.Float()),
					arguments(Double.class, Serdes.Double()),
					arguments(byte[].class, Serdes.ByteArray()),
					arguments(ByteBuffer.class, Serdes.ByteBuffer()),
					arguments(Bytes.class, Serdes.Bytes()),
					arguments(UUID.class, Serdes.UUID())
				);
			}

			@Nested
			class ForNonKafkaStreamsBuiltInType {

				@Test
				void returnsFallbackSerdeWhenValidFallbackSpecified() {
					contextRunner.run((context) ->
						assertThat(SerdeResolverUtils.resolveForType(context, ResolvableType.forClass(Foo.class), fallback))
							.isSameAs(fallback));
				}

				@ParameterizedTest
				@MethodSource("invalidFallbackSerdeProvider")
				void returnsJsonSerdeWhenInvalidFallbackSpecified(Serde<?> invalidFallback) {
					contextRunner.run((context) ->
						assertThat(SerdeResolverUtils.resolveForType(context, ResolvableType.forClass(Foo.class), invalidFallback))
							.isInstanceOf(JsonSerde.class));
				}

				static Stream<Serde<?>> invalidFallbackSerdeProvider() {
					return Stream.of(
						Serdes.String(),
						Serdes.Short(),
						Serdes.Integer(),
						Serdes.Long(),
						Serdes.Float(),
						Serdes.Double(),
						Serdes.ByteArray(),
						Serdes.ByteBuffer(),
						Serdes.Bytes(),
						Serdes.UUID()
					);
				}

				@Test
				void returnsJsonSerdeWhenFallbackNotSpecified() {
					contextRunner.run((context ->
						assertThat(SerdeResolverUtils.resolveForType(context, ResolvableType.forClass(Foo.class), null))
							.isInstanceOf(JsonSerde.class)));
				}

				@Test
				void returnsFallbackSerdeForJavaLangObject() {
					// This is an edge case as the only way to get to the JsonSerde step in the 1st place is when
					// no fallback is specified or a fallback is specified but its invalid (for a built-in type).
					// We will use the 'fallback is not specified' scenario for this test.
					contextRunner.run((context) ->
						assertThat(SerdeResolverUtils.resolveForType(context, ResolvableType.forClass(Object.class), null))
							.isNull());
				}
			}
		}
	}

	@Nested
	class BeanNamesForMatchingSerdes {

		private ApplicationContextRunner contextRunner = new ApplicationContextRunner()
				.withPropertyValues("spring.cloud.function.ineligible-definitions: sendToDlqAndContinue");

		@Test
		void returnsNoSerdesForWildcardType() {
			this.contextRunner.withUserConfiguration(SerdeResolverSimpleTestApp.class)
				.run((context) -> {
					ResolvableType wildcardType = ResolvableType.forClass(Serde.class).getGeneric(0);
					assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, wildcardType)).isEmpty();
				});
		}

		/**
		 *
		 * Verify that {@link SerdeResolverUtils#beanNamesForMatchingSerdes} returns the proper serdes in the proper order
		 * for the following grid:
		 * <p><br>
		 * <i>NOTE:</i> {@code GE = GenericEvent}
		 * <p><pre>{@code
		 * ------------------------------------------------------------------
		 * KStream type       | Serde type
		 * ------------------------------------------------------------------
		 *                    | GE<Date> | GE<? extends Date> | GE<?> | GE
		 * ------------------------------------------------------------------
		 * GE<Date>           | 1        | -                  | -     | -
		 * GE<? extends Date> | 2        | 1                  | -     | -
		 * GE<?>              | 4        | 2                  | 1     | 3
		 * GE                 | 1        | -                  | -     | 2
		 * ------------------------------------------------------------------
		 * }</pre>
		 * <p><br>
		 * <i>NOTE:</i> On the last row, one might expect the {@code GE} serde to be the top match with the {@code GE}
		 * kstream. However, that is not the case because {@code GE} is a parameterized type and therefore when
		 * specified as a raw type (without any type info) it resolves to {@code GE<?>} which throws off the ordering.
		 * A best practice is to specify the type info (even if it is wildcard) for KStream parameterized types.
		 */
		@Test
		void returnsProperlyOrderedSerdesForSimpleGenericTypes() {

			ResolvableType geDate = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Date>>() { });
			ResolvableType geBounded = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Date>>() { });
			ResolvableType geWildcard = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<?>>() { });
			ResolvableType geRaw = ResolvableType.forRawClass(GenericEvent.class);

			this.contextRunner.withUserConfiguration(SerdeResolverSimpleTestApp.class)
				.run((context) -> {

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geDate))
					.containsExactly(
						"geDateSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geBounded))
					.containsExactly(
						"geDateBoundedSerde",
						"geDateSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geWildcard))
					.containsExactly(
						"geWildcardSerde",
						"geDateBoundedSerde",
						"geRawSerde",
						"geDateSerde",
						"geStringSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geRaw))
					.containsExactly(
						"geDateSerde",
						"geStringSerde",
						"geRawSerde");
			});
		}

		/**
		 * Verify that {@link SerdeResolverUtils#beanNamesForMatchingSerdes} returns the proper serdes in the proper order
		 * for the following grid:
		 * <p><br>
		 * <i>NOTE:</i> {@code GE = GenericEvent}
		 * <p><pre>{@code
		 * -------------------------------------------------------------------------------------------------------------------
		 * KStream type | Serde type
		 * -------------------------------------------------------------------------------------------------------------------
		 *              | GE<F<D>> | GE<F<?D>> | GE<F<?>> | GE<F> | GE<?F<D>> | GE<?F<?D>> | GE<?F<?>> | GE<?F> | GE<?> | GE
		 * -------------------------------------------------------------------------------------------------------------------
		 * GE<F<D>>     | 1        | -         | -        | -     | -         | -          | -         | -      | -     | -
		 * GE<F<?D>>    | 2        | 1         | -        | -     | -         | -          | -         | -      | -     | -
		 * GE<F<?>>     | 4        | 3         | 1        | 2     | -         | -          | -         | -      | -     | -
		 * GE<F>        | 2        | -         | -        | 1     | -         | -          | -         | -      | -     | -
		 * GE<?F<D>>    | 2        | -         | -        | -     | 1         | -          | -         | -      | -     | -
		 * GE<?F<?D>>   | 3        | 4         | -        | -     | 2         | 1          | -         | -      | -     | -
		 * GE<?F<?>>    | 7        | 8         | 5        | 6     | 4         | 3          | 1         | 2      | -     | -
		 * GE<?F>       | 4        | -         | -        | 3     | 2         | -          | -         | 1      | -     | -
		 * GE<?>        | 7        | 8         | 9        | 10    | 2         | 3          | 4         | 5      | 1     | 6
		 * GE           | 1        | 2         | 3        | 4     | -         | -          | -         | -      | -     | 5
		 * -------------------------------------------------------------------------------------------------------------------
		 * }</pre>
		 * <p><br>
		 * <i>NOTE:</i> On the last row, one might expect the {@code GE} serde to be the top match with the {@code GE}
		 * kstream. However, that is not the case because {@code GE} is a parameterized type and therefore when
		 * specified as a raw type (without any type info) it resolves to {@code GE<?>} which throws off the ordering.
		 * A best practice is to specify the type info (even if it is wildcard) for KStream parameterized types.
		 */
		@Test
		void returnsProperlyOrderedSerdesForComplexGenericTypes() {

			ResolvableType geFooDate = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Foo<Date>>>() { });
			ResolvableType geFooDateBounded = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Foo<? extends Date>>>() { });
			ResolvableType geFooWildcard = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Foo<?>>>() { });
			ResolvableType geFooRaw = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<Foo>>() { });
			ResolvableType geFooBoundedDate = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Foo<Date>>>() { });
			ResolvableType geFooBoundedDateBounded = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Foo<? extends Date>>>() { });
			ResolvableType geFooBoundedWildcard = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Foo<?>>>() { });
			ResolvableType geFooBoundedRaw = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<? extends Foo>>() { });
			ResolvableType geWildcard = ResolvableType.forType(new ParameterizedTypeReference<GenericEvent<?>>() { });
			ResolvableType geRaw = ResolvableType.forRawClass(GenericEvent.class);

			this.contextRunner.withUserConfiguration(SerdeResolverComplexTestApp.class)
				.run((context) -> {

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooDate))
					.containsExactly(
						"geFooDateSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooDateBounded))
					.containsExactly(
						"geFooDateBoundedSerde",
						"geFooDateSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooWildcard))
					.containsExactly(
						"geFooWildcardSerde",
						"geFooRawSerde",
						"geFooDateBoundedSerde",
						"geFooDateSerde",
						"geFooStringSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooRaw))
					.containsExactly(
						"geFooRawSerde",
						"geFooDateSerde",
						"geFooStringSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooBoundedDate))
					.containsExactly(
						"geFooBoundedDateSerde",
						"geFooDateSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooBoundedDateBounded))
					.containsExactly(
						"geFooBoundedDateBoundedSerde",
						"geFooBoundedDateSerde",
						"geFooDateSerde",
						"geFooDateBoundedSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooBoundedWildcard))
					.containsExactly(
						"geFooBoundedWildcardSerde",
						"geFooBoundedRawSerde",
						"geFooBoundedDateBoundedSerde",
						"geFooBoundedDateSerde",
						"geFooBoundedStringSerde",
						"geFooWildcardSerde",
						"geFooRawSerde",
						"geFooDateSerde",
						"geFooDateBoundedSerde",
						"geFooStringSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geFooBoundedRaw))
					.containsExactly(
						"geFooBoundedRawSerde",
						"geFooBoundedDateSerde",
						"geFooBoundedStringSerde",
						"geFooRawSerde",
						"geFooDateSerde",
						"geFooStringSerde");

				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geWildcard))
					.containsExactly(
						"geWildcardSerde",
						"geFooBoundedDateSerde",
						"geFooBoundedDateBoundedSerde",
						"geFooBoundedStringSerde",
						"geFooBoundedWildcardSerde",
						"geFooBoundedRawSerde",
						"geRawSerde",
						"geFooDateSerde",
						"geFooDateBoundedSerde",
						"geFooStringSerde",
						"geFooWildcardSerde",
						"geFooRawSerde");

				// One might expect geRawSerde to win in order, but it does not because GE is a parameterized type and
				// therefore GE resolves to GE<?> which throws off the ordering. Bottom line, for parameterized types
				// be sure to specify a type (even if it's wildcard) in the KStream<T> definition.
				assertThat(SerdeResolverUtils.beanNamesForMatchingSerdes(context, geRaw))
					.containsExactly(
						"geFooDateSerde",
						"geFooDateBoundedSerde",
						"geFooStringSerde",
						"geFooWildcardSerde",
						"geFooRawSerde",
						"geRawSerde");
			});
		}
	}

	static class GenericEventSerde<T> implements Serde<GenericEvent<? extends T>> {
		private String name;

		GenericEventSerde(String name) {
			this.name = name;
		}

		@Override
		public Serializer<GenericEvent<? extends T>> serializer() {
			return null;
		}

		@Override
		public Deserializer<GenericEvent<? extends T>> deserializer() {
			return null;
		}

		@Override
		public String toString() {
			return "GenericEventSerde(" + name + ")";
		}
	}

	static class GenericEvent<T> { }

	static class FooSerde implements Serde<Foo> {

		@Override
		public Serializer<Foo> serializer() {
			return null;
		}

		@Override
		public Deserializer<Foo> deserializer() {
			return null;
		}
	}

	static class Foo<T> { }

	@EnableAutoConfiguration
	static class SerdeResolverSimpleTestApp {

		@Bean
		public Serde<GenericEvent<Date>> geDateSerde() {
			return new GenericEventSerde("geDateSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Date>> geDateBoundedSerde() {
			return new GenericEventSerde("geDateBoundedSerde");
		}

		@Bean
		public Serde<GenericEvent<String>> geStringSerde() {
			return new GenericEventSerde("geStringSerde");
		}

		@Bean
		public Serde<GenericEvent<?>> geWildcardSerde() {
			return new GenericEventSerde("geWildcardSerde");
		}

		@Bean
		public Serde<GenericEvent> geRawSerde() {
			return new GenericEventSerde("geRawSerde");
		}

		@Bean
		public Serde<?> widlcardSerde() {
			return Serdes.Void();
		}

		@Bean
		public Serde<Foo> fooSerde() {
			return new FooSerde();
		}
	}

	@EnableAutoConfiguration
	static class SerdeResolverComplexTestApp {

		@Bean
		public Serde<GenericEvent<Foo<Date>>> geFooDateSerde() {
			return new GenericEventSerde("geFooDateSerde");
		}

		@Bean
		public Serde<GenericEvent<Foo<? extends Date>>> geFooDateBoundedSerde() {
			return new GenericEventSerde("geFooDateBoundedSerde");
		}

		@Bean
		public Serde<GenericEvent<Foo<String>>> geFooStringSerde() {
			return new GenericEventSerde("geFooStringSerde");
		}

		@Bean
		public Serde<GenericEvent<Foo<?>>> geFooWildcardSerde() {
			return new GenericEventSerde("geFooWildcardSerde");
		}

		@Bean
		public Serde<GenericEvent<Foo>> geFooRawSerde() {
			return new GenericEventSerde("geFooRawSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Foo<Date>>> geFooBoundedDateSerde() {
			return new GenericEventSerde("geFooBoundedDateSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Foo<? extends Date>>> geFooBoundedDateBoundedSerde() {
			return new GenericEventSerde("geFooBoundedDateBoundedSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Foo<String>>> geFooBoundedStringSerde() {
			return new GenericEventSerde("geFooBoundedStringSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Foo<?>>> geFooBoundedWildcardSerde() {
			return new GenericEventSerde("geFooBoundedWildcardSerde");
		}

		@Bean
		public Serde<GenericEvent<? extends Foo>> geFooBoundedRawSerde() {
			return new GenericEventSerde("geFooBoundedRawSerde");
		}

		@Bean
		public Serde<GenericEvent<?>> geWildcardSerde() {
			return new GenericEventSerde("geWildcardSerde");
		}

		@Bean
		public Serde<GenericEvent> geRawSerde() {
			return new GenericEventSerde("geRawSerde");
		}

		@Bean
		public Serde<?> widlcardSerde() {
			return Serdes.Void();
		}

		@Bean
		public Serde<Foo> fooSerde() {
			return new FooSerde();
		}
	}

}
