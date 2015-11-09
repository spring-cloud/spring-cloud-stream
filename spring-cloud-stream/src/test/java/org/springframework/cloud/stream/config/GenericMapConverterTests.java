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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsMapContaining.*;
import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;

/**
 * Tests for GenericMapConverter.
 *
 * @author Eric Bottard
 */
public class GenericMapConverterTests {

	@Test
	public void noDelegateMapConversion() {
		GenericConversionService conversionService = new GenericConversionService();
		GenericMapConverter mapConverter = new GenericMapConverter(conversionService);
		conversionService.addConverter(mapConverter);

		@SuppressWarnings("unchecked")
		Map<String, String> result = conversionService.convert("foo = bar, wizz = jogg", Map.class);
		assertThat(result, hasEntry("foo", "bar"));
		assertThat(result, hasEntry("wizz", "jogg"));

		assertThat(conversionService.canConvert(TypeDescriptor.valueOf(String.class),
						TypeDescriptor.map(Map.class, TypeDescriptor.valueOf(GenericMapConverterTests.class), TypeDescriptor.valueOf(Integer.class))),
				is(false));
	}

	@Test
	public void testDelegatingConversion() {
		DefaultConversionService conversionService = new DefaultConversionService();
		GenericMapConverter mapConverter = new GenericMapConverter(conversionService);
		conversionService.addConverter(mapConverter);

		TypeDescriptor targetType = TypeDescriptor.map(Map.class, TypeDescriptor.valueOf(Charset.class), TypeDescriptor.valueOf(UUID.class));
		@SuppressWarnings("unchecked")
		Map<Charset, UUID> result = (Map<Charset, UUID>) conversionService.convert("UTF-8 = c46c18c7-b000-44d0-8326-f12ddf423972", targetType);

		assertThat(result, hasEntry(Charset.forName("UTF-8"), UUID.fromString("c46c18c7-b000-44d0-8326-f12ddf423972")));
	}

	@Test
	public void testEscapes() {
		DefaultConversionService conversionService = new DefaultConversionService();
		GenericMapConverter mapConverter = new GenericMapConverter(conversionService);
		conversionService.addConverter(mapConverter);

		TypeDescriptor targetType = TypeDescriptor.map(Map.class, TypeDescriptor.valueOf(String.class), TypeDescriptor.valueOf(String[].class));
		@SuppressWarnings("unchecked")
		Map<String, String[]> result = (Map<String, String[]>) conversionService.convert("foo = bar\\,quizz", targetType);

		assertThat(result.values().iterator().next(), Matchers.arrayContaining("bar", "quizz"));

	}

}