/*
 * Copyright 2016-present the original author or authors.
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


package org.springframework.cloud.stream.binder;

import org.springframework.expression.Expression;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * since 5.0.2
 * author Oleg Zhurakousky
 */
public class ExpressionSerializer extends StdSerializer<Expression> {

	public ExpressionSerializer() {
		super(Expression.class);
	}
	public ExpressionSerializer(Class<Expression> t) {
		super(t);
	}
	@Override
	public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializationContext provider) throws JacksonException {
		if (expression != null) {
			jsonGenerator.writeString(expression.getExpressionString());
		}
	}
}
