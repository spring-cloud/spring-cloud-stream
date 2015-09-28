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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.core.convert.converter.Converter;

/**
 * Converter for Strings to Date that can take into account date patterns
 * 
 * @author Mark Pollack
 * 
 */
public class StringToDateConverter implements Converter<String, Date> {

	private final static String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";

	private DateFormat dateFormat;

	public StringToDateConverter() {
		this.dateFormat = new SimpleDateFormat(DEFAULT_DATE_PATTERN);
		this.dateFormat.setLenient(false);

	}

	public StringToDateConverter(String pattern) {
		this.dateFormat = new SimpleDateFormat(pattern);
		this.dateFormat.setLenient(false);
	}

	public StringToDateConverter(DateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}

	@Override
	public Date convert(String source) {
		try {
			return dateFormat.parse(source);
		}
		catch (ParseException e) {
			String pattern;
			if (dateFormat instanceof SimpleDateFormat) {
				pattern = ((SimpleDateFormat) dateFormat).toPattern();
			}
			else {
				pattern = dateFormat.toString();
			}
			throw new IllegalArgumentException(e.getMessage() + ", format: [" + pattern + "]");
		}
	}

}
