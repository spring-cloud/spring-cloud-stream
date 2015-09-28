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

package org.springframework.cloud.stream.tuple.integration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.integration.transformer.AbstractPayloadTransformer;
import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converts from a json string into a tuple data structure.
 * 
 * @author Mark Fisher
 */
public class JsonToTupleTransformer extends AbstractPayloadTransformer<String, Tuple> {

	private final ObjectMapper mapper = new ObjectMapper();

	public JsonToTupleTransformer() {
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
	}

	@Override
	public Tuple transformPayload(String json) throws Exception {
		List<String> names = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		JsonNode node = this.mapper.readTree(json);
		Iterator<String> fieldNames = node.fieldNames();
		while (fieldNames.hasNext()) {
			String name = fieldNames.next();
			JsonNode valueNode = node.get(name);
			Object value = mapper.treeToValue(valueNode, Object.class);
			names.add(name);
			values.add(value);
		}
		return TupleBuilder.tuple().ofNamesAndValues(names, values);
	}

}
