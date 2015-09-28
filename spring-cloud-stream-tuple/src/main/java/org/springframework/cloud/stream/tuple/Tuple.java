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

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import org.springframework.core.convert.ConversionFailedException;

/**
 * Data structure that stores a fixed number of ordered key-value pairs and adds convenience methods to access values in
 * a type-safe manner.
 * 
 * The structure is immutable once created and values to not need to be of the same type. When accessing values,
 * Spring's type conversion system is used to convert the value type to the requested type. The type conversion system
 * is extensible.
 * 
 * Tuples are created using the TupleBuilder class.
 * 
 * @author Mark Pollack
 * 
 */
public interface Tuple {

	/**
	 * Return the number of elements in this tuple.
	 * 
	 * @return number of elements
	 */
	int size();

	/**
	 * Return the fields names that can reference elements by name in this tuple
	 * 
	 * @return list of field names
	 */
	List<String> getFieldNames();

	/**
	 * Return the values for all the fields in this tuple
	 * 
	 * @return list of values.
	 */
	List<Object> getValues();

	/**
	 * Return true if the tuple contains a field of the given name
	 * 
	 * @param name the name of the field
	 * @return true if present, otherwise false
	 */
	boolean hasFieldName(String name);

	/**
	 * Return the Java types of the fields in this tuple.
	 * 
	 * @return the Java types of the fields in this tuple.
	 */
	@SuppressWarnings("rawtypes")
	List<Class> getFieldTypes();

	/**
	 * Return the number of fields in this tuple.
	 * 
	 * @return the number of fields in this tuple.
	 */
	int getFieldCount();

	/**
	 * Return the value of the field given the name
	 * 
	 * @param name the name of the field
	 * @return value of the field
	 * @throws IllegalArgumentException if the name is not present
	 */
	Object getValue(String name);

	/**
	 * Return the value of the field given the index position
	 * 
	 * @param index position in the tuple
	 * @return value of the field
	 * @throws IndexOutOfBoundsException if the index position is out of bounds.
	 */
	Object getValue(int index);

	/**
	 * Return the value of the field given the name
	 * 
	 * @param name the field name
	 * @param valueClass Class to coerce the value into.
	 * @return value of the field
	 */
	<T> T getValue(String name, Class<T> valueClass);

	/**
	 * Return the value of the field given the index position
	 * 
	 * @param index position in the tuple
	 * @param valueClass Class to coerce the value into
	 * @return value of the field
	 */
	<T> T getValue(int index, Class<T> valueClass);

	/**
	 * Read the {@link String} value given the field '<code>name</code>'.
	 * 
	 * @param name the field name.
	 * @return value of the field
	 */
	String getString(String name);

	/**
	 * Read the String value given the index position
	 * 
	 * @param index position in the tuple
	 * @return value of the field
	 */
	String getString(int index);

	/**
	 * Read the {@link Tuple} value given the field '<code>name</code>'.
	 * 
	 * @param name the field name.
	 * @return value of the field
	 */
	Tuple getTuple(String name);

	/**
	 * Read the Tuple value given the index position
	 * 
	 * @param index position in the tuple
	 * @return value of the field
	 */
	Tuple getTuple(int index);

	/**
	 * Read the {@link String} value at index '<code>index</code>' including trailing whitespace (don't trim).
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	String getRawString(int index);

	/**
	 * Read the {@link String} value from column with given '<code>name</code>' including trailing whitespace (don't
	 * trim).
	 * 
	 * @param name the field name.
	 */
	String getRawString(String name);

	/**
	 * Read the '<code>char</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	char getChar(int index);


	/**
	 * Read the '<code>char</code>' value from field with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 * @throws IllegalArgumentException if a field with given name is not defined.
	 */
	char getChar(String name);

	/**
	 * Read the '<code>boolean</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	boolean getBoolean(int index);

	/**
	 * Read the '<code>boolean</code>' value from field with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 * @throws IllegalArgumentException if a field with given name is not defined.
	 */
	boolean getBoolean(String name);

	/**
	 * Read the '<code>boolean</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @param trueValue the value that signifies {@link Boolean#TRUE true}; case-sensitive.
	 * @throws IndexOutOfBoundsException if the index is out of bounds, or if the supplied <code>trueValue</code> is
	 *         <code>null</code>.
	 */
	boolean getBoolean(int index, String trueValue);

	/**
	 * Read the '<code>boolean</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 * @param trueValue the value that signifies {@link Boolean#TRUE true}; case-sensitive.
	 * @throws IllegalArgumentException if a column with given name is not defined, or if the supplied
	 *         <code>trueValue</code> is <code>null</code>.
	 */
	boolean getBoolean(String name, String trueValue);

	/**
	 * Read the '<code>byte</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	byte getByte(int index);

	/**
	 * Read the '<code>byte</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	byte getByte(String name);


	/**
	 * Read the '<code>byte</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code> if
	 * the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	byte getByte(int index, byte defaultValue);

	/**
	 * Read the '<code>byte</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	byte getByte(String name, byte defaultValue);

	/**
	 * Read the '<code>short</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	short getShort(int index);

	/**
	 * Read the '<code>short</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	short getShort(String name);

	/**
	 * Read the '<code>short</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code>
	 * if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	short getShort(int index, short defaultValue);

	/**
	 * Read the '<code>short</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	short getShort(String name, short defaultValue);

	/**
	 * Read the '<code>int</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	int getInt(int index);

	/**
	 * Read the '<code>int</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	int getInt(String name);

	/**
	 * Read the '<code>int</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code> if
	 * the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	int getInt(int index, int defaultValue);

	/**
	 * Read the '<code>int</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	int getInt(String name, int defaultValue);

	/**
	 * Read the '<code>long</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	long getLong(int index);

	/**
	 * Read the '<code>int</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	long getLong(String name);

	/**
	 * Read the '<code>long</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code> if
	 * the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	long getLong(int index, long defaultValue);

	/**
	 * Read the '<code>long</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	long getLong(String name, long defaultValue);

	/**
	 * Read the '<code>float</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	float getFloat(int index);

	/**
	 * Read the '<code>float</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	float getFloat(String name);

	/**
	 * Read the '<code>float</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code>
	 * if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	float getFloat(int index, float defaultValue);

	/**
	 * Read the '<code>float</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	float getFloat(String name, float defaultValue);


	/**
	 * Read the '<code>double</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	double getDouble(int index);

	/**
	 * Read the '<code>double</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	double getDouble(String name);


	/**
	 * Read the '<code>double</code>' value at index '<code>index</code>'. using the supplied <code>defaultValue</code>
	 * if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	double getDouble(int index, double defaultValue);

	/**
	 * Read the '<code>double</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	double getDouble(String name, double defaultValue);

	/**
	 * Read the '<code>BigDecimal</code>' value at index '<code>index</code>'.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	BigDecimal getBigDecimal(int index);

	/**
	 * Read the '<code>BigDecimal</code>' value from column with given '<code>name</code>'.
	 * 
	 * @param name the field name.
	 */
	BigDecimal getBigDecimal(String name);

	/**
	 * Read the '<code>BigDecimal</code>' value at index '<code>index</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 */
	BigDecimal getBigDecimal(int index, BigDecimal defaultValue);

	/**
	 * Read the '<code>BigDecimal</code>' value from column with given '<code>name</code>'. using the supplied
	 * <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 */
	BigDecimal getBigDecimal(String name, BigDecimal defaultValue);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column <code>index</code>.
	 * 
	 * @param index the field index.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 * @throws ConversionFailedException if the value is not parseable
	 */
	Date getDate(int index);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column with given <code>name</code>.
	 * 
	 * @param name the field name.
	 * @throws IllegalArgumentException if a column with given name is not defined
	 * @throws ConversionFailedException if the value is not parseable
	 */
	Date getDate(String name);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column <code>index</code> using the
	 * supplied <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 * @throws ConversionFailedException if the value is not parseable
	 */
	Date getDate(int index, Date defaultValue);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column with given <code>name</code>.
	 * using the supplied <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IllegalArgumentException if a column with given name is not defined
	 * @throws ConversionFailedException if the value is not parseable
	 */
	Date getDate(String name, Date defaultValue);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column <code>index</code>.
	 * 
	 * @param index the field index.
	 * @param pattern the pattern describing the date and time format
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 * @throws IllegalArgumentException if the date cannot be parsed.
	 * 
	 */
	Date getDateWithPattern(int index, String pattern);

	/**
	 * Read the <code>java.util.Date</code> value in given format from column with given <code>name</code>.
	 * 
	 * @param name the field name.
	 * @param pattern the pattern describing the date and time format
	 * @throws IllegalArgumentException if a column with given name is not defined or if the specified field cannot be
	 *         parsed
	 * 
	 */
	Date getDateWithPattern(String name, String pattern);

	/**
	 * Read the <code>java.util.Date</code> value in default format at designated column <code>index</code>. using the
	 * supplied <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param index the field index.
	 * @param pattern the pattern describing the date and time format
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IndexOutOfBoundsException if the index is out of bounds.
	 * @throws IllegalArgumentException if the date cannot be parsed.
	 * 
	 */
	Date getDateWithPattern(int index, String pattern, Date defaultValue);

	/**
	 * Read the <code>java.util.Date</code> value in given format from column with given <code>name</code>. using the
	 * supplied <code>defaultValue</code> if the field value is a zero length string or null.
	 * 
	 * @param name the field name.
	 * @param pattern the pattern describing the date and time format
	 * @param defaultValue the default value to return if field value is not found.
	 * @throws IllegalArgumentException if a column with given name is not defined or if the specified field cannot be
	 *         parsed
	 * 
	 */
	Date getDateWithPattern(String name, String pattern, Date defaultValue);

	/**
	 * Use SpEL expression to return a subset of the tuple that matches the expression
	 * 
	 * @param expression SpEL expression to select from a Map, e.g. ?[key.startsWith('b')]
	 * @return a new Tuple with data selected from the current instance.
	 */
	Tuple select(String expression);

}
