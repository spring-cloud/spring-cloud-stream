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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author David Turanski
 *
 */
public class JsonNodeToTupleConverter implements Converter<JsonNode, Tuple> {

	@Override
	public Tuple convert(JsonNode root) {
		TupleBuilder builder = TupleBuilder.tuple();
		if (root.isValueNode()) {
			return builder.of("value", root.asText());
		}
		try {
			for (Iterator<Entry<String, JsonNode>> it = root.fields(); it.hasNext();) {
				Entry<String, JsonNode> entry = it.next();
				String name = entry.getKey();
				JsonNode node = entry.getValue();
				if (node.isObject()) {
					// tuple
					builder.addEntry(name, convert(node));
				}
				else if (node.isArray()) {
					builder.addEntry(name, nodeToList(node));
				}
				else {
					if (name.equals("id")) {//NOSONAR
						// TODO how should this be handled?
					}
					else if (name.equals("timestamp")) {//NOSONAR
						// TODO how should this be handled?
					}
					else {
						builder.addEntry(name, node.asText());
					}
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return builder.build();
	}

	private List<Object> nodeToList(JsonNode node) {
		List<Object> list = new ArrayList<Object>(node.size());
		for (int i = 0; i < node.size(); i++) {
			JsonNode item = node.get(i);
			if (item.isObject()) {
				list.add(convert(item));
			}
			else if (item.isArray()) {
				list.add(nodeToList(item));
			}
			else {
				list.add(item.asText());
			}
		}
		return list;
	}

}
