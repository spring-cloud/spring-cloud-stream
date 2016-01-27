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

package org.springframework.cloud.stream.tuple.spel;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Unit tests for {@link TuplePropertyAccessor}.
 *
 * @author Eric Bottard
 * @author Mark Fisher
 */
public class TuplePropertyAccessorTests {

	private final SpelExpressionParser parser = new SpelExpressionParser();

	private final StandardEvaluationContext context = new StandardEvaluationContext();

	@Before
	public void setup() {
		this.context.addPropertyAccessor(new TuplePropertyAccessor());
	}

	@Test
	public void testSimplePropertyByFieldName() {
		Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
		String result = evaluate("foo", tuple, String.class);
		assertThat(result, is("bar"));
	}

	@Test
	public void testSimplePropertyByIndex() {
		Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
		String result = evaluate("['0']", tuple, String.class);
		assertThat(result, is("bar"));
	}

	@Test(expected = SpelEvaluationException.class)
	public void failOnNegativeIndex() {
		Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
		evaluate("['-3']", tuple, String.class);
	}

	@Test
	public void testNestedPropertyByFieldName() {
		Tuple child = TupleBuilder.tuple().of("b", 123);
		Tuple tuple = TupleBuilder.tuple().of("a", child);
		int result = evaluate("a.b", tuple, Integer.class);
		assertThat(result, is(123));
	}

	@Test
	public void testNestedPropertyByIndex() {
		Tuple child = TupleBuilder.tuple().of("b", 123);
		Tuple tuple = TupleBuilder.tuple().of("a", child);
		int result = evaluate("a['0']", tuple, Integer.class);
		assertThat(result, is(123));
	}

	@Test
	public void testNestedPropertyByIndexOnly() {
		Tuple child = TupleBuilder.tuple().of("b", 123);
		Tuple tuple = TupleBuilder.tuple().of("a", child);
		int result = evaluate("['0']['0']", tuple, Integer.class);
		assertThat(result, is(123));
	}

	@Test
	public void testArrayPropertyByFieldName() {
		Tuple tuple = TupleBuilder.tuple().of("numbers", new Integer[] { 1, 2, 3 });
		int result = evaluate("numbers[1]", tuple, Integer.class);
		assertThat(result, is(2));
	}

	@Test
	public void testArrayPropertyByIndex() {
		Tuple tuple = TupleBuilder.tuple().of("numbers", new Integer[] { 1, 2, 3 });
		int result = evaluate("['0'][0]", tuple, Integer.class);
		assertThat(result, is(1));
	}

	@Test
	public void testNestedArrayPropertyByFieldName() {
		Tuple child = TupleBuilder.tuple().of("numbers", new Integer[] { 7, 8, 9 });
		Tuple tuple = TupleBuilder.tuple().of("child", child);
		int result = evaluate("child.numbers[1]", tuple, Integer.class);
		assertThat(result, is(8));
	}

	@Test
	public void testNestedArrayPropertyByIndex() {
		Tuple child = TupleBuilder.tuple().of("numbers", new Integer[] { 7, 8, 9 });
		Tuple tuple = TupleBuilder.tuple().of("child", child);
		int result = evaluate("child['0'][2]", tuple, Integer.class);
		assertThat(result, is(9));
	}

	@Test
	public void testNestedArrayPropertyByIndexOnly() {
		Tuple child = TupleBuilder.tuple().of("numbers", new Integer[] { 7, 8, 9 });
		Tuple tuple = TupleBuilder.tuple().of("child", child);
		int result = evaluate("['0']['0'][1]", tuple, Integer.class);
		assertThat(result, is(8));
	}

	// Write

	@Test
	public void testSimpleWriteOnExistingProperty() {
		Tuple tuple = TupleBuilder.mutableTuple().of("foo", "bar");
		write("foo", tuple, 123);
		assertThat(tuple.getInt("foo"), is(123));
	}

	@Test
	public void testSimpleWriteOnNewProperty() {
		Tuple tuple = TupleBuilder.mutableTuple().of("foo", "bar");
		write("other", tuple, 123);
		assertThat(tuple.getInt("other"), is(123));
		assertThat(tuple.getValue("foo"), CoreMatchers.<Object>is("bar"));
	}

	@Test
	public void testSimpleWriteOnExistingPropertyByIndex() {
		Tuple tuple = TupleBuilder.mutableTuple().of("foo", "bar");
		write("['0']", tuple, 123);
		assertThat(tuple.getInt("foo"), is(123));
	}

	@Test
	public void testSimpleWriteOnNewPropertyByIndex() {
		Tuple tuple = TupleBuilder.mutableTuple().of("foo", "bar");
		write("['12']", tuple, 123);
		assertThat(tuple.getInt("12"), is(123));
		assertThat(tuple.getValue("foo"), CoreMatchers.<Object>is("bar"));
	}

	private <T> T evaluate(String expression, Tuple tuple, Class<T> expectedType) {
		return parser.parseExpression(expression).getValue(this.context, tuple, expectedType);
	}

	private void write(String expression, Tuple tuple, Object value) {
		parser.parseExpression(expression).setValue(this.context, tuple, value);
	}

}
