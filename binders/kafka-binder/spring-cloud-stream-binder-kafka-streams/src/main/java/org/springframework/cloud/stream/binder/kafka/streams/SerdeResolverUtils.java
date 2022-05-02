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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.ClassUtils;

/**
 * Utility class that contains various methods to help resolve {@link Serde Serdes}.
 *
 * @author Chris Bono
 * @since 4.0
 */
abstract class SerdeResolverUtils {

	private static final Log LOG = LogFactory.getLog(SerdeResolverUtils.class);

	/**
	 *  Return the closest matching configured {@code Serde<?>} bean if one exists, or the specified
	 *  {@code fallbackSerde}, or finally a standard default serde if no fallback specified.
	 *
	 * @param context the application context
	 * @param targetType the target type to find the serde for
	 * @param fallbackSerde the fallback serde in case no matching serde bean found in the context
	 * @return serde to use for the target type
	 */
	static Serde<?> resolveForType(ConfigurableApplicationContext context, ResolvableType targetType, Serde<?> fallbackSerde) {

		List<Serde<?>> matchingSerdes = findMatchingSerdes(context, targetType);
		if (!matchingSerdes.isEmpty()) {
			return matchingSerdes.get(0);
		}

		// We don't attempt to find a matching Serde for type '?'
		if (targetType.getRawClass() == null) {
			return null;
		}

		Serde<?> serde = null;
		Class<?> genericRawClazz = targetType.getRawClass();
		if (Integer.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.Integer();
		}
		else if (Long.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.Long();
		}
		else if (Short.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.Short();
		}
		else if (Double.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.Double();
		}
		else if (Float.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.Float();
		}
		else if (byte[].class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.ByteArray();
		}
		else if (String.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.String();
		}
		else if (UUID.class.isAssignableFrom(genericRawClazz)) {
			serde = Serdes.UUID();
		}
		else if (!isSerdeFromStandardDefaults(fallbackSerde)) {
			// User purposely set a default serde that is not one of the above
			serde = fallbackSerde;
		}
		else {
			// If the type is Object, then skip assigning the JsonSerde and let the fallback mechanism takes precedence.
			if (!genericRawClazz.isAssignableFrom((Object.class))) {
				serde = new JsonSerde(genericRawClazz);
			}
		}
		return serde;
	}

	private static boolean isSerdeFromStandardDefaults(Serde<?> serde) {
		if (serde != null) {
			if (Number.class.isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.ByteArray().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.String().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.UUID().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Find all {@link Serde} beans that are assignable from {@code targetType}.
	 *
	 * @param context the application context
	 * @param targetType the target type the serdes are being matched for
	 * @return list of matching serdes order by most specific match, or an empty list if no matches found
	 */
	static List<Serde<?>> findMatchingSerdes(ConfigurableApplicationContext context, ResolvableType targetType) {
		// We don't attempt to find a matching Serde for type '?'
		if (targetType.getRawClass() == null) {
			return Collections.emptyList();
		}
		List<SerdeWithSpecificityScore> matchingSerdes = new ArrayList<>();

		context.getBeansOfType(Serde.class).forEach((beanName, serdeBean) -> {
			final Class<?> beanConfigClass = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
					context.getBeanFactory().getBeanDefinition(beanName))
					.getMetadata().getClassName(),
				ClassUtils.getDefaultClassLoader());
			try {
				Method[] methods = beanConfigClass.getMethods();
				Optional<Method> serdeBeanMethod = Arrays.stream(methods).filter(m -> m.getName().equals(beanName)).findFirst();
				serdeBeanMethod.ifPresent((method) -> {
					ResolvableType serdeBeanMethodReturnType = ResolvableType.forMethodReturnType(method, beanConfigClass);
					ResolvableType serdeBeanGeneric = serdeBeanMethodReturnType.getGeneric(0);
					// We don't attempt to use a Serde<?> as a match for anything currently
					if (serdeBeanGeneric.getRawClass() != null && serdeBeanGeneric.isAssignableFrom(targetType)) {
						matchingSerdes.add(new SerdeWithSpecificityScore(calculateScore(targetType, serdeBeanGeneric), serdeBean));
					}
				});
			}
			catch (Exception e) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Failed to introspect Serde bean method '" + serdeBean + "'", e);
				}
			}
		});
		if (!matchingSerdes.isEmpty()) {
			return matchingSerdes.stream().sorted(Collections.reverseOrder())
				.map(SerdeWithSpecificityScore::getSerde)
				.collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	/**
	 * Calculate a score to indicate how specific of a match one resolvable type is to another.
	 * <p><br>Simple string comparison (the number of matching leading characters between two type strings) is used to
	 * calculate the score. This approach avoids the recursive nature of the possible generic types, and leverages
	 * the type strings returned from {@link ResolvableType#toString()} and {@link Type#getTypeName()} which already
	 * include the properly handled generic types. The score is a composite of both of these properties because the
	 * 'toString' value does not include bounds values and works as a tie-breaker to distinguish non-exact
	 * matches that are closer in nature.
	 * <p><br><b>Example:</b>
	 * <pre>{@code
	 * -------------------------------------------------------------------------------------------------------
	 * targetType: Foo<Date>               toString='Foo<Date>'     typeName='Foo<java.util.Date>'
	 * typeToCheck1: Foo<Date>             toString='Foo<Date>'     typeName='Foo<java.util.Date>'
	 * typeToCheck2: Foo<? extends Date>   toString='Foo<Date>'     typeName='Foo<? extends java.util.Date>'
	 * -------------------------------------------------------------------------------------------------------
	 * }</pre>
	 *
	 * If using only the 'toString' value then both types would have the same score. However, including the 'typeName'
	 * value in the score differentiates them - in this case it is clear that 'typeToCheck1' is a direct match.
	 *
	 * @param targetType the target type
	 * @param typeToScore the type to calculate a score for
	 * @return a score on how close of a match {@code typeToScore} is to {@code targetType} - the higher the score,
	 * 	the closer the match
	 */
	private static int calculateScore(ResolvableType targetType, ResolvableType typeToScore) {
		int score = countLeadingMatchingChars(targetType.getType().getTypeName(), typeToScore.getType().getTypeName());
		score += countLeadingMatchingChars(targetType.toString(), typeToScore.toString());
		return score;
	}

	private static int countLeadingMatchingChars(String s1, String s2) {
		if (s1 == null || s2 == null) {
			return 0;
		}
		int matchCount = 0;
		for (int i = 0; i < s1.length() && i < s2.length(); i++) {
			if (s1.charAt(i) != s2.charAt(i)) {
				break;
			}
			matchCount++;
		}
		return matchCount;
	}

	/**
	 * Private internal class used strictly to 'remember' a score for a serde and use it for sorting later.
	 */
	private static class SerdeWithSpecificityScore implements Comparable<SerdeWithSpecificityScore> {
		private Integer score;
		private Serde<?> serde;

		SerdeWithSpecificityScore(Integer score, Serde<?> serde) {
			this.score = Objects.requireNonNull(score);
			this.serde = Objects.requireNonNull(serde);
		}

		Serde<?> getSerde() {
			return serde;
		}

		@Override
		public int compareTo(SerdeWithSpecificityScore other) {
			return this.score.compareTo(other.score);
		}
	}

}
