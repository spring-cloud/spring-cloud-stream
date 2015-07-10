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

package org.springframework.xd.dirt.integration.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author David Turanski
 * 
 */
public abstract class AbstractRedisSerializerTests {

	private RedisSerializer<Object> serializer;

	protected abstract RedisSerializer<Object> getSerializer();

	@Before
	public void setUp() {
		serializer = getSerializer();
	}

	@Test
	public void testRandomObjectSerialization() {
		Foo foo = new Foo("hello");
		byte[] bytes = serializer.serialize(foo);
		Object obj = serializer.deserialize(bytes);
		assertTrue(obj instanceof Foo);
		assertEquals("hello", ((Foo) obj).bar);
	}

	@Test
	public void testDateSerialization() {
		Date d = new Date();
		byte[] bytes = serializer.serialize(d);
		Date obj = (Date) serializer.deserialize(bytes);
		assertEquals(d, obj);
	}

	@Test
	public void testDateTimeSerialization() {
		DateTime d = new DateTime();
		byte[] bytes = serializer.serialize(d);
		DateTime obj = (DateTime) serializer.deserialize(bytes);
		assertEquals(d, obj);
	}

	@Test
	public void testStringSerialization() {
		String s = new String("hello");
		byte[] bytes = serializer.serialize(s);
		Object obj = serializer.deserialize(bytes);
		assertEquals(s, obj);
	}

	@Test
	public void testLongSerialization() {
		byte[] bytes = serializer.serialize(100L);
		long obj = (Long) serializer.deserialize(bytes);
		assertEquals(100, obj);
	}

	@Test
	public void testFloatSerialization() {
		byte[] bytes = serializer.serialize(99.9f);
		double obj = (Double) serializer.deserialize(bytes);
		assertEquals(99.9, obj, 0.1);
	}

	@Test
	public void testBooleanSerialization() {
		byte[] bytes = serializer.serialize(true);
		boolean obj = (Boolean) serializer.deserialize(bytes);
		assertTrue(obj);
	}

	@Test
	public void testMapSerialization() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("foo", "bar");
		byte[] bytes = serializer.serialize(map);
		Map<?, ?> obj = (Map<?, ?>) serializer.deserialize(bytes);
		assertEquals("bar", obj.get("foo"));
	}

	@Test
	public void testListSerialization() {
		List<String> list = new LinkedList<String>();
		list.add("foo");
		byte[] bytes = serializer.serialize(list);
		List<?> obj = (List<?>) serializer.deserialize(bytes);
		assertEquals("foo", obj.get(0));
	}

	@Test
	public void testSetSerialization() {
		Set<String> set = new TreeSet<String>();
		set.add("foo");
		byte[] bytes = serializer.serialize(set);
		Set<?> obj = (Set<?>) serializer.deserialize(bytes);
		assertEquals("foo", obj.iterator().next());
	}

	@Test
	public void testTupleSerialization() {
		Tuple t = TupleBuilder.tuple().of("foo", "bar");
		byte[] bytes = serializer.serialize(t);

		Tuple obj = (Tuple) serializer.deserialize(bytes);
		assertEquals("bar", obj.getString("foo"));
	}

	public static class Foo {

		@JsonCreator
		public Foo(@JsonProperty("bar") String val) {
			bar = val;
		}

		public String bar;
	}

}
