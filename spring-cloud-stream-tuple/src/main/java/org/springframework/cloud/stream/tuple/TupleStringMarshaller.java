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

package org.springframework.cloud.stream.tuple;

import org.springframework.core.convert.converter.Converter;

/**
 * @author David Turanski
 * 
 */
public class TupleStringMarshaller {

	private final Converter<Tuple, String> tupleToStringConverter;

	private final Converter<String, Tuple> stringToTupleConverter;

	public TupleStringMarshaller(Converter<Tuple, String> tupleToStringConverter,
			Converter<String, Tuple> stringToTupleConverter) {
		this.tupleToStringConverter = tupleToStringConverter;
		this.stringToTupleConverter = stringToTupleConverter;
	}

	public Tuple toTuple(String source) {
		return stringToTupleConverter.convert(source);
	}

	public String fromTuple(Tuple source) {
		return tupleToStringConverter.convert(source);
	}
}
