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

package org.springframework.cloud.stream.tuple;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.cloud.stream.tuple.TupleBuilder.tuple;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;

import org.springframework.core.convert.ConversionFailedException;

/**
 * This is a port of the FieldSet tests from Spring Batch
 * 
 */
public class DefaultTupleTestForBatch {

	Tuple tuple;

	List<Object> values;

	List<String> names;


	@Before
	public void setUp() throws Exception {

		Object[] tokens = new String[] { "TestString", "true", "C", "10", "-472", "354224", "543", "124.3", "424.3",
			"1,3245",
			null, "2007-10-12", "12-10-2007", "" };
		String[] nameArray = new String[] { "String", "Boolean", "Char", "Byte", "Short", "Integer", "Long", "Float",
			"Double",
			"BigDecimal", "Null", "Date", "DatePattern", "BlankInput" };

		names = Arrays.asList(nameArray);
		values = Arrays.asList(tokens);
		/*
		 * values = new ArrayList<Object>(); for (String token : tokens) { values.add(token); }
		 */
		tuple = tuple().ofNamesAndValues(names, values);
		assertEquals(14, tuple.size());

	}

	@Test
	public void testNames() throws Exception {
		// MLP - tuples always have names, FieldSet in Spring Batch doesn't require a name.
		assertThat(tuple.getFieldCount(), is(tuple.getFieldNames().size()));
	}

	@Test
	public void testReadString() {
		assertThat(tuple.getString(0), is("TestString"));
		assertThat(tuple.getString("String"), is("TestString"));
	}

	@Test
	public void testReadChar() {
		assertThat(tuple.getChar(2), is('C'));
		assertThat(tuple.getChar("Char"), is('C'));
	}

	@Test
	public void testReadBooleanTrue() {
		assertThat(tuple.getBoolean(1), is(true));
		assertThat(tuple.getBoolean("Boolean"), is(true));
	}

	@Test
	public void testReadByte() {
		assertTrue(tuple.getByte(3) == 10);
		assertTrue(tuple.getByte("Byte") == 10);
	}

	@Test
	public void testReadShort() {
		assertTrue(tuple.getShort(4) == -472);
		assertTrue(tuple.getShort("Short") == -472);
	}

	@Test
	public void testReadIntegerAsFloat() {
		assertEquals(354224, tuple.getFloat(5), .001);
		assertEquals(354224, tuple.getFloat("Integer"), .001);
	}

	@Test
	public void testReadFloat() throws Exception {
		assertTrue(tuple.getFloat(7) == 124.3F);
		assertTrue(tuple.getFloat("Float") == 124.3F);
	}

	@Test
	public void testReadIntegerAsDouble() throws Exception {
		assertEquals(354224, tuple.getDouble(5), .001);
		assertEquals(354224, tuple.getDouble("Integer"), .001);
	}

	@Test
	public void testReadDouble() throws Exception {
		assertTrue(tuple.getDouble(8) == 424.3);
		assertTrue(tuple.getDouble("Double") == 424.3);
	}

	@Test
	public void testReadBigDecimal() throws Exception {
		BigDecimal bd = new BigDecimal("424.3");
		assertEquals(bd, tuple.getBigDecimal(8));
		assertEquals(bd, tuple.getBigDecimal("Double"));
	}

	@Test
	public void testReadBigBigDecimal() throws Exception {
		BigDecimal bd = new BigDecimal("12345678901234567890");
		Tuple tuple = TupleBuilder.tuple().of("bigd", "12345678901234567890");
		assertEquals(bd, tuple.getBigDecimal(0));
	}

	@Test
	public void testReadBigDecimalWithFormat() throws Exception {
		Tuple numberFormatTuple = TupleBuilder.tuple()
				.setFormats(Locale.US, null)
				.setConfigurableConversionService(new DefaultTupleConversionService())
				.ofNamesAndValues(tuple.getFieldNames(), tuple.getValues());
		BigDecimal bd = new BigDecimal("424.3");
		assertEquals(bd, numberFormatTuple.getBigDecimal(8));
	}

	@Test
	public void testReadBigDecimalWithEuroFormat() throws Exception {
		Tuple numberFormatTuple = TupleBuilder.tuple()
				.setFormats(Locale.GERMANY, null)
				.setConfigurableConversionService(new DefaultTupleConversionService())
				.ofNamesAndValues(tuple.getFieldNames(), tuple.getValues());
		BigDecimal bd = new BigDecimal("1.3245");
		assertEquals(bd, numberFormatTuple.getBigDecimal(9));
	}

	@Test
	public void testReadNonExistentField() {
		String s = tuple.getString("something");
		assertThat(s, nullValue());
	}

	@Test
	public void testReadIndexOutOfRange() {
		try {
			tuple.getShort(-1);
			fail("field set returns value even index is out of range!");
		}
		catch (IndexOutOfBoundsException e) {
			assertTrue(true);
		}

		try {
			tuple.getShort(99);
			fail("field set returns value even index is out of range!");
		}
		catch (Exception e) {
			assertTrue(true);
		}
	}

	@Test
	public void testReadBooleanWithTrueValue() {
		assertTrue(tuple.getBoolean(1, "true"));
		assertFalse(tuple.getBoolean(1, "incorrect trueValue"));

		assertTrue(tuple.getBoolean("Boolean", "true"));
		assertFalse(tuple.getBoolean("Boolean", "incorrect trueValue"));
	}

	@Test
	public void testReadBooleanFalse() {
		Tuple t = TupleBuilder.tuple().of("foo", false);
		assertFalse(t.getBoolean(0));
	}

	@Test
	public void testReadCharException() {
		try {
			tuple.getChar(1);
			fail("the value read was not a character, exception expected");
		}
		catch (IllegalArgumentException expected) {
			assertTrue(true);
		}

		try {
			tuple.getChar("Boolean");
			fail("the value read was not a character, exception expected");
		}
		catch (IllegalArgumentException expected) {
			assertTrue(true);
		}
	}


	@Test
	public void testReadInt() throws Exception {
		assertThat(354224, equalTo(tuple.getInt(5)));
		assertThat(354224, equalTo(tuple.getInt("Integer")));
	}

	@Test
	public void testReadIntWithSeparator() {
		Tuple t = TupleBuilder.tuple().of("foo", "354,224");
		assertThat(354224, equalTo(t.getInt(0)));
	}

	@Test
	public void testReadIntWithSeparatorAndFormat() throws Exception {
		Tuple t = TupleBuilder.tuple()
				.setFormats(Locale.GERMAN, null)
				.setConfigurableConversionService(new DefaultTupleConversionService())
				.of("foo", "354.224");
		assertThat(354224, equalTo(t.getInt(0)));
	}

	@Test
	public void testReadBlankInt() {

		// Trying to parse a blank field as an integer, but without a default
		// value should throw a NumberFormatException
		try {
			tuple.getInt(13);
			fail();
		}
		catch (ConversionFailedException ex) {
			// expected
		}

		try {
			tuple.getInt("BlankInput");
			fail();
		}
		catch (ConversionFailedException ex) {
			// expected
		}

	}

	@Test
	public void testReadLong() throws Exception {
		assertThat(543L, equalTo(tuple.getLong(6)));
		assertThat(543L, equalTo(tuple.getLong("Long")));
	}

	@Test
	public void testReadLongWithPadding() throws Exception {
		Tuple t = TupleBuilder.tuple().of("foo", "000009");
		assertThat(9L, equalTo(t.getLong(0)));
	}

	@Test
	public void testReadIntWithNullValue() {
		assertThat(5, equalTo(tuple.getInt(10, 5)));
		assertThat(5, equalTo(tuple.getInt("Null", 5)));
	}

	@Test
	public void testReadIntWithDefaultAndNotNull() {
		assertThat(354224, equalTo(tuple.getInt(5, 5)));
		assertThat(354224, equalTo(tuple.getInt("Integer", 5)));
	}

	@Test
	public void testReadLongWithNullValue() {
		long defaultValue = 5;
		int indexOfNull = 10;
		int indexNotNull = 6;
		String nameNull = "Null";
		String nameNotNull = "Long";
		long longValueAtIndex = 543;

		assertThat(defaultValue, equalTo(tuple.getLong(indexOfNull, defaultValue)));
		assertThat(longValueAtIndex, equalTo(tuple.getLong(indexNotNull, defaultValue)));

		assertThat(defaultValue, equalTo(tuple.getLong(nameNull, defaultValue)));
		assertThat(longValueAtIndex, equalTo(tuple.getLong(nameNotNull, defaultValue)));
	}

	@Test
	public void testReadBigDecimalInvalid() {
		int index = 0;

		try {
			tuple.getBigDecimal(index);
			fail("field value is not a number, exception expected");
		}
		catch (ConversionFailedException e) {
			assertTrue(e.getMessage().indexOf("TestString") > 0);
		}

	}

	@Test
	public void testReadBigDecimalByNameInvalid() throws Exception {
		try {
			tuple.getBigDecimal("String");
			fail("field value is not a number, exception expected");
		}
		catch (ConversionFailedException e) {
			assertTrue(e.getMessage().indexOf("TestString") > 0);
		}
	}


	@Test
	public void testReadDate() throws Exception {
		assertNotNull(tuple.getDate(11));
		assertNotNull(tuple.getDate("Date"));
	}

	@Test
	public void testReadDateWithDefault() {
		Date date = null;
		assertEquals(date, tuple.getDateWithPattern(13, "dd-MM-yyyy", date));
		assertEquals(date, tuple.getDateWithPattern("BlankInput", "dd-MM-yyyy", date));
	}

	@Test
	public void testReadDateWithFormat() throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Tuple t = TupleBuilder.tuple()
				.setFormats(null, dateFormat)
				.setConfigurableConversionService(new DefaultTupleConversionService())
				.of("foo", "13/01/1999");
		assertEquals(dateFormat.parse("13/01/1999"), t.getDate(0));
	}

	@Test
	public void testReadDateInvalid() throws Exception {
		try {
			tuple.getDate(0);
			fail("field value is not a date, exception expected");
		}
		catch (ConversionFailedException e) {
			assertTrue(e.getMessage().indexOf("TestString") > 0);
		}

	}

	@Test
	public void testReadDateInvalidByName() throws Exception {

		try {
			tuple.getDate("String");
			fail("field value is not a date, exception expected");
		}
		catch (ConversionFailedException e) {
			assertTrue(e.getMessage().indexOf("TestString") > 0);
			// assertTrue(e.getMessage().indexOf("name: [String]") > 0);
		}

	}

	@Test
	public void testReadDateInvalidWithPattern() throws Exception {

		try {
			tuple.getDateWithPattern(0, "dd-MM-yyyy");
			fail("field value is not a date, exception expected");
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().indexOf("dd-MM-yyyy") > 0);
		}
	}

	@Test
	public void testReadDateWithPatternAndDefault() {
		Date date = null;
		assertEquals(date, tuple.getDateWithPattern(13, "dd-MM-yyyy", date));
		assertEquals(date, tuple.getDateWithPattern("BlankInput", "dd-MM-yyyy", date));
	}

	@Test
	public void testStrictReadDateWithPattern() throws Exception {

		Tuple t = tuple().of("foo", "50-2-13");
		try {
			t.getDateWithPattern(0, "dd-MM-yyyy");
			fail("field value is not a valid date for strict parser, exception expected");
		}
		catch (IllegalArgumentException e) {
			String message = e.getMessage();
			assertTrue("Message did not contain: " + message, message.indexOf("dd-MM-yyyy") > 0);
		}
	}

	@Test
	public void testStrictReadDateWithPatternAndStrangeDate() throws Exception {

		Tuple t = tuple().of("foo", "5550212");
		try {
			System.err.println(t.getDateWithPattern(0, "yyyyMMdd"));
			fail("field value is not a valid date for strict parser, exception expected");
		}
		catch (IllegalArgumentException e) {
			String message = e.getMessage();
			assertTrue("Message did not contain: " + message, message.indexOf("yyyyMMdd") > 0);
		}
	}

	@Test
	public void testReadDateByNameInvalidWithPattern() throws Exception {

		try {
			tuple.getDateWithPattern("String", "dd-MM-yyyy");
			fail("field value is not a date, exception expected");
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().indexOf("dd-MM-yyyy") > 0);
			assertTrue(e.getMessage().indexOf("String") > 0);
		}
	}

	@Test
	public void testPaddedLong() {
		Tuple t = tuple().of("foo", "00000009");
		// FieldSet fs = new DefaultFieldSet(new String[] { "00000009" });

		long value = t.getLong(0);
		assertEquals(value, 9);
	}

	@Test
	public void testReadRawString() {
		String name = "fieldName";
		String value = " string with trailing whitespace   ";
		Tuple t = tuple().of(name, value);

		assertEquals(value, t.getRawString(0));
		assertEquals(value, t.getRawString(name));
	}
}
