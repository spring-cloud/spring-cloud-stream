/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.tuple.spel;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.cloud.stream.tuple.Tuple;

/**
 * A {@link PropertyAccessor} implementation that enables reading of {@link Tuple} values using dot notation within SpEL
 * expressions. Writing is not supported since {@link Tuple}s are immutable.
 * 
 * @author Mark Fisher
 */
public class TuplePropertyAccessor implements PropertyAccessor {

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] { Tuple.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		Tuple tuple = (Tuple) target;
		if (tuple.hasFieldName(name)) {
			return true;
		}
		return maybeIndex(name, tuple) != null;
	}

	/**
	 * Return an integer if the String property name can be parsed as an int, or null otherwise.
	 */
	private Integer maybeIndex(String name, Tuple tuple) {
		Integer index = null;
		try {
			int i = Integer.parseInt(name);
			if (i > -1 && tuple.size() > i) {
				index = i;
			}
		}
		catch (NumberFormatException e) {
			// not an integer
		}
		return index;
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		Tuple tuple = (Tuple) target;
		boolean hasKey = false;
		Object value = null;
		if (tuple.hasFieldName(name)) {
			hasKey = true;
			value = tuple.getValue(name);
		}
		else {
			Integer index = maybeIndex(name, tuple);
			if (index != null) {
				hasKey = true;
				value = tuple.getValue(index);
			}
		}
		if (value == null && !hasKey) {
			throw new TupleAccessException(name);
		}
		return new TypedValue(value);
	}

	@Override
	public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
		return false;
	}

	@Override
	public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {
		throw new UnsupportedOperationException("Tuple is immutable");
	}

	/**
	 * Exception thrown from {@code read} in order to reset a cached PropertyAccessor, allowing other accessors to have
	 * a try.
	 */
	@SuppressWarnings("serial")
	private static class TupleAccessException extends AccessException {

		private final String name;

		public TupleAccessException(String name) {
			super(null);
			this.name = name;
		}

		@Override
		public String getMessage() {
			return "Tuple does not contain a value for field name '" + this.name + "'";
		}
	}

}
