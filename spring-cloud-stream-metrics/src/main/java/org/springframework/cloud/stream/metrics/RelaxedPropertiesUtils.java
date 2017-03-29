/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics;

import java.util.TreeSet;
import java.util.regex.Pattern;

import org.springframework.boot.bind.RelaxedNames;
import org.springframework.util.StringUtils;

/**
 * Utility class to deal with {@link RelaxedNames}.
 *
 * @author Vinicius Carvalho
 */
class RelaxedPropertiesUtils {

	private static final Pattern HYPHEN_LOWER = Pattern.compile("-_|_-|__|\\.-|\\._");

	private static final Pattern SEPARATED_TO_CAMEL_CASE_PATTERN = Pattern
			.compile("[_\\-.]");

	private static final char[] SUFFIXES = new char[] { '_', '-', '.' };

	/**
	 * Searches relaxed names and tries to find a best match for a canonical form using
	 * dot notation. For example, if a new list was built with JAVA_HOME as property, the
	 * return of this method would be {@code java.home}.
	 *
	 * Relaxed names generate a long list of variations of a property, it can be tricky
	 * trying to infer the correct format, which sometimes may not even exist in dot
	 * notation.
	 *
	 * This method attempts to find a best match, or if none is found it converts
	 * underscore format to dot notation.
	 *
	 * @param names list of possible permutations of a property
	 * @return the canonical form (dot notation) of this property
	 */
	public static String findCanonicalFormat(Iterable<String> names) {
		TreeSet<String> sorted = new TreeSet<>();
		String environmentFormat = null;
		String simpleFormat = null;
		for (String name : names) {
			sorted.add(name);
		}
		String canonicalForm = null;
		for (String name : sorted) {
			if (HYPHEN_LOWER.matcher(name).find()) {
				continue;
			}
			if (upperCaseRatio(name) == 1.0) {
				if (name.contains("_")) {
					environmentFormat = name;
				}
				if (!name.matches("^.*?(_|-).*$")) {
					simpleFormat = name;
				}
				continue;
			}
			if (name.contains(".")) {
				String[] keys = name.split("\\.");
				for (int i = 0; i < keys.length; i++) {
					keys[i] = separatedToCamelCase(keys[i], false);
				}
				canonicalForm = StringUtils.arrayToDelimitedString(keys, ".");
				break;
			}
		}
		// If we can't find any variation it could mean we have a camelCase only value
		// such as springApplicationName.
		// In this case RelaxedNames only generate _ values, so we should get one and
		// transform into dot notation.
		// Another possibility is a top level property such as MEM or OS, in this case
		// there's no separator and we only set to lowercase
		if (canonicalForm == null) {
			if (environmentFormat != null) {
				canonicalForm = environmentFormat.toLowerCase().replace("_", ".");
			}
			if (canonicalForm == null) {
				if (simpleFormat != null) {
					canonicalForm = simpleFormat.toLowerCase();
				}
			}
		}
		return canonicalForm;
	}

	/**
	 * Returns the ratio of uppercase chars on a string
	 * @param input
	 * @return
	 */
	private static double upperCaseRatio(String input) {
		int upperCaseCount = 0;
		String compare = input.replaceAll("[._-]", "");
		for (int i = 0; i < compare.length(); i++) {
			if (Character.isUpperCase(compare.charAt(i))) {
				upperCaseCount++;
			}
		}
		return (float) upperCaseCount / compare.length();
	}

	/**
	 * Taken from {@link RelaxedNames}, convert an input of type string-string into
	 * stringString
	 * @param value
	 * @param caseInsensitive
	 * @return
	 */
	private static String separatedToCamelCase(String value, boolean caseInsensitive) {
		if (value.isEmpty()) {
			return value;
		}
		StringBuilder builder = new StringBuilder();
		for (String field : SEPARATED_TO_CAMEL_CASE_PATTERN.split(value)) {
			field = (caseInsensitive ? field.toLowerCase() : field);
			builder.append(builder.length() == 0 ? field : StringUtils.capitalize(field));
		}
		char lastChar = value.charAt(value.length() - 1);
		for (char suffix : SUFFIXES) {
			if (lastChar == suffix) {
				builder.append(suffix);
				break;
			}
		}
		return builder.toString();
	}

}
