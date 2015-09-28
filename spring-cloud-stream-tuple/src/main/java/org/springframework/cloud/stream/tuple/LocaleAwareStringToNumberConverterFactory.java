/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.cloud.stream.tuple;

import java.text.NumberFormat;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.util.NumberUtils;

/**
 * Converts from a String any JDK-standard Number implementation.
 * 
 * <p>
 * Support Number classes including Byte, Short, Integer, Float, Double, Long, BigInteger, BigDecimal. This class
 * delegates to {@link NumberUtils#parseNumber(String, Class, NumberFormat)} to perform the conversion.
 * 
 * @author Mark Pollack
 * @author Keith Donald
 * 
 * @see Byte
 * @see Short
 * @see Integer
 * @see Long
 * @see java.math.BigInteger
 * @see Float
 * @see Double
 * @see java.math.BigDecimal
 * @see NumberUtils
 */
public class LocaleAwareStringToNumberConverterFactory implements ConverterFactory<String, Number> {

	private NumberFormat numberFormat;

	public LocaleAwareStringToNumberConverterFactory(NumberFormat numberFormat) {
		this.numberFormat = numberFormat;
	}

	@Override
	public <T extends Number> Converter<String, T> getConverter(Class<T> targetType) {
		return new StringToNumber<T>(targetType, numberFormat);
	}

	private static final class StringToNumber<T extends Number> implements Converter<String, T> {

		private final Class<T> targetType;

		private NumberFormat numberFormat;

		public StringToNumber(Class<T> targetType, NumberFormat numberFormat) {
			this.targetType = targetType;
			this.numberFormat = numberFormat;
		}

		@Override
		public T convert(String source) {
			return NumberUtils.parseNumber(source, this.targetType, numberFormat);
		}
	}

}
