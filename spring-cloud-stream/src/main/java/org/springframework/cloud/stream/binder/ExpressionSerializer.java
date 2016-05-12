/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.springframework.expression.Expression;

/**
 * JSON serializer for Expression value.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ExpressionSerializer extends JsonSerializer<Expression> {

	public ExpressionSerializer() {
	}

	@Override
	public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
			throws IOException {
		if (expression != null) {
			jsonGenerator.writeString(expression.getExpressionString());
		}
	}
}
