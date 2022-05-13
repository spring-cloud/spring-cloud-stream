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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.lang.Nullable;

/**
 * Utility class that contains various methods to help resolve {@link Serde Serdes}.
 *
 * @author Chris Bono
 * @since 4.0
 */
abstract class SerdeResolverUtils {

	private static final Log LOG = LogFactory.getLog(SerdeResolverUtils.class);

	/** Classnames of the standard built-in Serdes supported in {@link Serdes#serdeFrom(Class)} */
	private static final Set<String> STANDARD_SERDE_CLASSNAMES = Set.of(
		Serdes.String().getClass().getName(),
		Serdes.Short().getClass().getName(),
		Serdes.Integer().getClass().getName(),
		Serdes.Long().getClass().getName(),
		Serdes.Float().getClass().getName(),
		Serdes.Double().getClass().getName(),
		Serdes.ByteArray().getClass().getName(),
		Serdes.ByteBuffer().getClass().getName(),
		Serdes.Bytes().getClass().getName(),
		Serdes.UUID().getClass().getName());
		
	/**
	 *  Return the {@code Serde<?>} to use for the specified type using the following steps until a match is found:
	 *  <p><ul>
	 *    <li>the closest matching configured {@code Serde<?>} bean if one exists</li>
	 *    <li>the Kafka Streams built-in serde if the target type is one of the built-in types exposed by Kafka Streams
	 *    (Integer, Long, Short, Double, Float, byte[], UUID and String)
	 *    <li>the fallback serde if specified and not one of the Kafka Streams exposed type serdes</li>
	 *	  <li>the {@link JsonSerde} if the target type is not exactly {@code Object}</li>
	 *	  <li>the fallback as the last resort</li>
	 *  </ul>
	 * @param context the application context
	 * @param targetType the target type to find the serde for
	 * @param fallbackSerde the serde to use when no type can be inferred
	 * @return serde to use for the target type or {@code fallbackSerde} as outlined in the method description
	 */
	static Serde<?> resolveForType(ConfigurableApplicationContext context, ResolvableType targetType, @Nullable Serde<?> fallbackSerde) {

		Class<?> genericRawClazz = targetType.getRawClass();

		// We don't attempt to find a matching Serde for type '?' - just return fallback
		if (genericRawClazz == null) {
			return fallbackSerde;
		}

		List<String> matchingSerdes = beanNamesForMatchingSerdes(context, targetType);
		if (!matchingSerdes.isEmpty()) {
			return context.getBean(matchingSerdes.get(0), Serde.class);
		}

		// Use standard serde for built-in types
		Serde<?> standardDefaultSerde = getStandardDefaultSerde(genericRawClazz);
		if (standardDefaultSerde != null) {
			return standardDefaultSerde;
		}

		// Use fallback if specified and not from std defaults (we know from above that type is not std default
		// so using a fallback that is std default type would not work)
		if (fallbackSerde != null && !isSerdeFromStandardDefaults(fallbackSerde)) {
			return fallbackSerde;
		}

		// Use JsonSerde if type is not exactly Object
		if (!genericRawClazz.isAssignableFrom((Object.class))) {
			return new JsonSerde<>(genericRawClazz);
		}

		// Finally, just resort to using the fallback
		return fallbackSerde;
	}

	private static Serde<?> getStandardDefaultSerde(Class<?> genericRawClazz) {
		try {
			return Serdes.serdeFrom(genericRawClazz);
		}
		catch (IllegalArgumentException ex) {
			if (LOG.isTraceEnabled()) {
				LOG.trace(ex);
			}
		}
		return null;
	}

	private static boolean isSerdeFromStandardDefaults(Serde<?> serde) {
		if (serde == null) {
			return false;
		}
		return STANDARD_SERDE_CLASSNAMES.contains(serde.getClass().getName());
	}

	/**
	 * Find the names of all {@link Serde} beans that can be used for {@code targetType}.
	 *
	 * @param context the application context
	 * @param targetType the target type the serdes are being matched for
	 * @return list of bean names for matching serdes ordered by most specific match, or an empty list if no matches found
	 */
	static List<String> beanNamesForMatchingSerdes(ConfigurableApplicationContext context, ResolvableType targetType) {
		// We don't attempt to find a matching Serde for type '?'
		if (targetType.getRawClass() == null) {
			return Collections.emptyList();
		}
		List<SerdeWithSpecificityScore> matchingSerdes = new ArrayList<>();
		ResolvableType serdeType = ResolvableType.forClassWithGenerics(Serde.class, targetType);
		String[] serdeBeanNames = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context.getBeanFactory(),
			serdeType, false, false);
		Arrays.stream(serdeBeanNames).forEach((beanName) -> {
			try {
				BeanDefinition beanDefinition = context.getBeanFactory().getMergedBeanDefinition(beanName);
				ResolvableType serdeBeanGeneric = beanDefinition.getResolvableType().getGeneric(0);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Found matching Serde<" + serdeBeanGeneric.getType() + "> under beanName=" + beanName);
				}
				matchingSerdes.add(new SerdeWithSpecificityScore(calculateScore(targetType, serdeBeanGeneric), beanName));
			}
			catch (Exception ex) {
				LOG.warn("Failed introspecting Serde bean '" + beanName + "'", ex);
			}
		});
		if (!matchingSerdes.isEmpty()) {
			return matchingSerdes.stream().sorted(Collections.reverseOrder())
				.map(SerdeWithSpecificityScore::getSerdeBeanName)
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
	 * targetType:   Foo<Date>             toString='Foo<Date>'     typeName='Foo<java.util.Date>'
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
		private String serdeBeanName;

		SerdeWithSpecificityScore(Integer score, String serdeBeanName) {
			this.score = Objects.requireNonNull(score);
			this.serdeBeanName = Objects.requireNonNull(serdeBeanName);
		}

		String getSerdeBeanName() {
			return serdeBeanName;
		}

		@Override
		public int compareTo(SerdeWithSpecificityScore other) {
			return this.score.compareTo(other.score);
		}
	}

}
