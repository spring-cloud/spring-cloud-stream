/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.schema.server.support;

import java.util.List;

import org.apache.avro.SchemaParseException;

import org.springframework.cloud.stream.schema.server.model.Compatibility;
import org.springframework.cloud.stream.schema.server.model.Schema;

/**
 * @author Vinicius Carvalho
 */
public class AvroSchemaValidator implements SchemaValidator {

	@Override
	public boolean isValid(String definition) {
		boolean result = true;
		try {
			new org.apache.avro.Schema.Parser().parse(definition);
		}
		catch (SchemaParseException ex) {
			result = false;
		}
		return result;
	}

	@Override
	public Compatibility compatibilityCheck(String source, String other) {
		return null;
	}

	@Override
	public Schema match(List<Schema> schemas, String definition) {
		Schema result = null;
		org.apache.avro.Schema source = new org.apache.avro.Schema.Parser()
				.parse(definition);
		for (Schema s : schemas) {
			org.apache.avro.Schema target = new org.apache.avro.Schema.Parser()
					.parse(s.getDefinition());
			if (target.equals(source)) {
				result = s;
				break;
			}
		}
		return result;
	}

	@Override
	public String getFormat() {
		return "avro";
	}

}
