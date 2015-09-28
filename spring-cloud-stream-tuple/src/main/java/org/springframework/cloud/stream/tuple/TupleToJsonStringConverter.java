/*
 * Copyright 2002-2015 the original author or authors.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Converts a Tuple to JSON representation
 *
 * @author David Turanski
 * @author Gunnar Hillert
 *
 */
public class TupleToJsonStringConverter implements Converter<Tuple, String> {

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public String convert(Tuple source) {
		ObjectNode root = toObjectNode(source);
		String json = null;
		try {
			json = mapper.writeValueAsString(root);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Tuple to string conversion failed", e);
		}
		return json;
	}

	private ObjectNode toObjectNode(Tuple source) {
		ObjectNode root = mapper.createObjectNode();
//		root.put("id", source.getId().toString());
//		root.put("timestamp", source.getTimestamp());
		for (int i = 0; i < source.size(); i++) {
			Object value = source.getValues().get(i);
			String name = source.getFieldNames().get(i);
			if (value != null) {
				if (value instanceof Tuple) {
					root.putPOJO(name, toObjectNode((Tuple) value));
				}
				else if (!value.getClass().isPrimitive()) {
					root.putPOJO(name, root.pojoNode(value));
				}
				else {
					root.put(name, value.toString());
				}
			}
		}
		return root;
	}

}
