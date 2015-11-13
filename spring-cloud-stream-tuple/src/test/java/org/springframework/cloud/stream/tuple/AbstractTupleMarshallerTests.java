/*
 * Copyright 2002-2013 the original author or authors.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.cloud.stream.tuple.TupleBuilder.tuple;

/**
 * @author David Turanski
 * 
 */
public abstract class AbstractTupleMarshallerTests {

	private TupleStringMarshaller marshaller;

	private ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setUp() {
		marshaller = getMarshaller();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	@Test
	public void testMarshallSimpleTuple() {

		DefaultTuple tuple = (DefaultTuple) tuple().of("foo", "bar", "int", 1);
		String result = marshaller.fromTuple(tuple);
		Tuple newTuple = marshaller.toTuple(result);
		assertEquals(2, newTuple.getFieldCount());
		assertEquals("bar", newTuple.getString(0));
		assertEquals(1, newTuple.getInt(1));
	}

	@Test
	public void testMarshallNestedTuple() {
		Tuple t1 = tuple().of("foo", "bar", "int", 1);
		Tuple tuple = tuple().of("t1", t1, "hello", "world");

		String result = marshaller.fromTuple(tuple);
		Tuple newTuple = marshaller.toTuple(result);

		assertEquals(2, newTuple.getFieldCount());
		assertEquals("world", newTuple.getString(1));

		Tuple nested = newTuple.getTuple(0);
		assertEquals(2, nested.getFieldCount());
		assertEquals("bar", nested.getString(0));
		assertEquals(1, nested.getInt(1));
	}

	@Test
	public void testMarshallTupleWithCollection() {
		List<?> values = Arrays.asList("a", "b", "c");
		Tuple tuple = tuple().of("list", values);

		String result = marshaller.fromTuple(tuple);
		Tuple newTuple = marshaller.toTuple(result);
		assertTrue(newTuple.getValue(0) instanceof List);
		List<?> list = (List<?>) newTuple.getValue(0);
		assertEquals(values.size(), list.size());
	}

	@Test
	public void testMarshallTupleWithMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("k1", "v1");
		map.put("k2", "v2");
		Tuple tuple = tuple().of("map", map);

		String result = marshaller.fromTuple(tuple);

		Tuple newTuple = marshaller.toTuple(result);
		assertNotNull(newTuple.getValue("map"));

		// Map is converted to a tuple
		assertEquals("v1", newTuple.getTuple("map").getString("k1"));
		assertEquals("v2", newTuple.getTuple("map").getString("k2"));
	}

	public String prettyPrintJson(String json) throws IOException {
		Object jsonObject = mapper.readValue(json, Object.class);
		return mapper.writeValueAsString(jsonObject);
	}

	public String readJson(Resource resource) throws IOException {
		Object jsonObject = mapper.readValue(resource.getInputStream(), Object.class);
		return mapper.writeValueAsString(jsonObject);
	}



	protected abstract TupleStringMarshaller getMarshaller();
}
