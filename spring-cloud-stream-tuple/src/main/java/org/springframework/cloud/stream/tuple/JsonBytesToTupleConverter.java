/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.cloud.stream.tuple;

import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author David Turanski
 * 
 */
public class JsonBytesToTupleConverter implements Converter<byte[], Tuple> {

	private final ObjectMapper mapper = new ObjectMapper();

	private final JsonNodeToTupleConverter jsonNodeToTupleConverter = new JsonNodeToTupleConverter();

	public JsonBytesToTupleConverter() {
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
	}

	@Override
	public Tuple convert(byte[] source) {
		if (source == null) {
			return null;
		}
		try {
			return jsonNodeToTupleConverter.convert(mapper.readTree(source));
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

}
