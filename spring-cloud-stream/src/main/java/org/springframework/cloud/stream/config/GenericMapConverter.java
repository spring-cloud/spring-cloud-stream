/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.util.Assert;

/**
 * A Converter that can convert from a csv String to {@literal Map<K, V>}, as long
 * as the ConversionService it delegates to can convert from String to {@literal K}
 * and {@literal V}.
 *
 * @author Eric Bottard
 */
public class GenericMapConverter implements ConditionalGenericConverter {

	private ConversionService conversionService;

	private final String pairsSeparator;

	private final String keyValueSeparator;

	public GenericMapConverter(ConversionService conversionService, String pairsSeparator, String keyValueSeparator) {
		this.conversionService = conversionService;
		this.pairsSeparator = pairsSeparator;
		this.keyValueSeparator = keyValueSeparator;
	}

	public GenericMapConverter(ConversionService conversionService) {
		this(conversionService, ",", "=");
	}

	@Override
	public Set<ConvertiblePair> getConvertibleTypes() {
		return Collections.singleton(new ConvertiblePair(String.class, Map.class));
	}

	@Override
	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
		TypeDescriptor keyType = targetType.getMapKeyTypeDescriptor();
		TypeDescriptor valueType = targetType.getMapValueTypeDescriptor();
		return (keyType == null || this.conversionService.canConvert(sourceType, keyType))
				&& (valueType == null || this.conversionService.canConvert(sourceType, valueType));
	}

	@Override
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		Map<Object, Object> result = new LinkedHashMap<>();
		TypeDescriptor keyType = targetType.getMapKeyTypeDescriptor();
		TypeDescriptor valueType = targetType.getMapValueTypeDescriptor();

		// Split on comma if not preceded by backslash
		String[] mappings = ((String) source).split("(?<!\\\\)" + pairsSeparator);
		for (String mapping : mappings) {
			// Turn backslash-comma back to comma
			String unescaped = unEscapeAndTrimPair(mapping);
			if (unescaped.length() == 0) {
				continue;
			}
			// Split on colon, if not preceded by backslash
			String[] keyValuePair = unescaped.split("(?<!\\\\)" + keyValueSeparator);
			Assert.isTrue(keyValuePair.length == 2, "'" + unescaped +
					"' could not be parsed to a 'key:value' pair");
			String key = unEscapeAndTrimKeyOrValue(keyValuePair[0]);
			String value = unEscapeAndTrimKeyOrValue(keyValuePair[1]);
			result.put(keyType == null ? key : conversionService.convert(key, sourceType, keyType),
					valueType == null ? value : conversionService.convert(value, sourceType, valueType));
		}
		return result;
	}

	private String unEscapeAndTrimPair(String mapping) {
		return mapping.trim().replace("\\" + pairsSeparator, pairsSeparator);
	}

	private String unEscapeAndTrimKeyOrValue(String s) {
		return s.trim().replace("\\" + keyValueSeparator, keyValueSeparator);
	}
}
