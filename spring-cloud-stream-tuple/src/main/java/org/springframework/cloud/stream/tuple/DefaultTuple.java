/*
 * Copyright 2013-2015 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default implementation of Tuple interface
 * @author Mark Pollack
 * @author David Turanski
 * @author Michael Minella
 */
public class DefaultTuple implements Tuple {

	private List<String> names;

	private List<Object> values;

	private transient ConfigurableConversionService configurableConversionService;

	private transient Converter<Tuple, String> tupleToStringConverter = new TupleToJsonStringConverter();

	public DefaultTuple(List<String> names, List<Object> values, ConfigurableConversionService 
			configurableConversionService) {
		Assert.notNull(names);
		Assert.notNull(values);
		Assert.notNull(configurableConversionService);
		if (values.size() != names.size()) {
			throw new IllegalArgumentException("Field names must be same length as values: names=" + names
					+ ", values=" + values);
		}
		this.names = new ArrayList<>(names);
		this.values = new ArrayList<>(values); // shallow copy
		this.configurableConversionService = configurableConversionService;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#size()
	 */
	@Override
	public int size() {
		return values.size();
	}

	/**
	 * Return the values for all the fields in this tuple
	 * @return an unmodifiable List of names.
	 */
	@Override
	public List<String> getFieldNames() {
		return Collections.unmodifiableList(names);
	}

	/**
	 * Return the values for all the fields in this tuple
	 * @return an unmodifiable List list of values.
	 */
	@Override
	public List<Object> getValues() {
		return Collections.unmodifiableList(values);
	}

	@Override
	public int getFieldCount() {
		return this.names.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#hasName(java.lang.String)
	 */
	@Override
	public boolean hasFieldName(String name) {
		return names.contains(name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#getValue(java.lang.String)
	 */
	@Override
	public Object getValue(String name) {
		int index = indexOf(name);
		if (index == -1) {
			throw new IllegalArgumentException("Field name [" + name + "] does not exist");
		}
		return getValue(index);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#getValue(int)
	 */
	@Override
	public Object getValue(int index) {
		return values.get(index);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<Class> getFieldTypes() {
		ArrayList<Class> types = new ArrayList<>(values.size());
		for (Object val : values) {
			types.add(val.getClass());
		}
		return Collections.unmodifiableList(types);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((names == null) ? 0 : names.hashCode());
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof DefaultTuple)) {
			return false;
		}
		DefaultTuple other = (DefaultTuple) obj;
		if (names == null) {
			if (other.names != null) {
				return false;
			}
		}
		else if (!names.equals(other.names)) {
			return false;
		}
		if (values == null) {
			if (other.values != null) {
				return false;
			}
		}
		else if (!values.equals(other.values)) {
			return false;
		}
		return true;
	}

	@Override
	public String getString(String name) {
		int index = indexOf(name);
		return (index == -1) ? null : getString(index);
	}

	@Override
	public String getString(int index) {
		return readAndTrim(index);
	}

	@Override
	public Tuple getTuple(int index) {
		return convert(values.get(index), Tuple.class);
	}

	@Override
	public Tuple getTuple(String name) {
		return getTuple(indexOf(name));
	}

	/**
	 * @param index the index of the value
	 * @return the converted raw value, trimmed
	 */
	private String readAndTrim(int index) {
		Object rawValue = values.get(index);
		if (rawValue != null) {
			String value = convert(rawValue, String.class);
			if (value != null) {
				return value.trim();
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}

	@Override
	public String getRawString(String name) {
		int index = indexOf(name);
		return (index == -1) ? null : getRawString(index);
	}

	@Override
	public String getRawString(int index) {
		Object rawValue = values.get(index);
		if (rawValue != null) {
			String value = convert(rawValue, String.class);
			if (value != null) {
				return value;
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}

	@Override
	public char getChar(int index) {
		String value = readAndTrim(index);
		if (value != null) {
			Assert.isTrue(value.length() == 1, "Cannot convert field value '" + value + "' to char.");
			return value.charAt(0);
		}
		return '\u0000';

	}

	@Override
	public char getChar(String name) {
		return getChar(indexOf(name));
	}

	@Override
	public boolean getBoolean(int index) {
		return getBoolean(index, "true");
	}

	@Override
	public boolean getBoolean(String name) {
		return getBoolean(indexOf(name));
	}

	@Override
	public boolean getBoolean(int index, String trueValue) {
		Assert.notNull(trueValue, "'trueValue' cannot be null.");
		String value = readAndTrim(index);
		return trueValue.equals(value);

	}

	@Override
	public boolean getBoolean(String name, String trueValue) {
		return getBoolean(indexOf(name), trueValue);
	}

	@Override
	public byte getByte(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getByte(index);
	}

	@Override
	public byte getByte(int index) {
		Byte b = convert(values.get(index), Byte.class);
		return (b != null) ? b : 0;
	}

	@Override
	public byte getByte(String name, byte defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getByte(index, defaultValue);
	}

	@Override
	public byte getByte(int index, byte defaultValue) {
		Byte b = convert(values.get(index), Byte.class);
		return (b != null) ? b : defaultValue;
	}

	@Override
	public short getShort(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getShort(index);
	}

	@Override
	public short getShort(int index) {
		Short s = convert(values.get(index), Short.class);
		return (s != null) ? s : 0;
	}

	@Override
	public short getShort(String name, short defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getShort(index, defaultValue);
	}

	@Override
	public short getShort(int index, short defaultValue) {
		Short s = convert(values.get(index), Short.class);
		return (s != null) ? s : defaultValue;
	}

	@Override
	public int getInt(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getInt(index);
	}

	@Override
	public int getInt(int index) {
		Integer i = convert(values.get(index), Integer.class);
		return (i != null) ? i : 0;
	}

	@Override
	public int getInt(String name, int defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getInt(index, defaultValue);
	}

	@Override
	public int getInt(int index, int defaultValue) {
		Integer i = convert(values.get(index), Integer.class);
		return (i != null) ? i : defaultValue;
	}

	@Override
	public long getLong(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getLong(index);
	}

	@Override
	public long getLong(int index) {
		Long l = convert(values.get(index), Long.class);
		return (l != null) ? l : 0;
	}

	@Override
	public long getLong(String name, long defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getLong(index, defaultValue);
	}

	@Override
	public long getLong(int index, long defaultValue) {
		Long l = convert(values.get(index), Long.class);
		return (l != null) ? l : defaultValue;
	}

	@Override
	public float getFloat(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getFloat(index);
	}

	@Override
	public float getFloat(int index) {
		Float f = convert(values.get(index), Float.class);
		return (f != null) ? f : 0;
	}

	@Override
	public float getFloat(String name, float defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getFloat(index, defaultValue);
	}

	@Override
	public float getFloat(int index, float defaultValue) {
		Float f = convert(values.get(index), Float.class);
		return (f != null) ? f : defaultValue;
	}

	@Override
	public double getDouble(String name) {
		int index = indexOf(name);
		return (index == -1) ? 0 : getDouble(index);
	}

	@Override
	public double getDouble(int index) {
		Double d = convert(values.get(index), Double.class);
		return (d != null) ? d : 0;
	}

	@Override
	public double getDouble(String name, double defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getDouble(index, defaultValue);
	}

	@Override
	public double getDouble(int index, double defaultValue) {
		Double d = convert(values.get(index), Double.class);
		return (d != null) ? d : defaultValue;
	}

	@Override
	public BigDecimal getBigDecimal(String name) {
		return getBigDecimal(indexOf(name));
	}

	@Override
	public BigDecimal getBigDecimal(int index) {
		return convert(values.get(index), BigDecimal.class);
	}

	@Override
	public BigDecimal getBigDecimal(String name, BigDecimal defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getBigDecimal(index, defaultValue);
	}

	@Override
	public BigDecimal getBigDecimal(int index, BigDecimal defaultValue) {
		BigDecimal bd = convert(values.get(index), BigDecimal.class);
		return (bd != null) ? bd : defaultValue;
	}

	@Override
	public Date getDate(int index) {
		return convert(values.get(index), Date.class);
	}

	@Override
	public Date getDate(String name) {
		return getDate(indexOf(name));
	}

	@Override
	public Date getDate(String name, Date defaultValue) {
		int index = indexOf(name);
		return (index == -1) ? defaultValue : getDate(index, defaultValue);
	}

	@Override
	public Date getDate(int index, Date defaultValue) {
		Date d = getDate(index);
		return (d != null) ? d : defaultValue;
	}

	@Override
	public Date getDateWithPattern(int index, String pattern) {
		StringToDateConverter converter = new StringToDateConverter(pattern);
		return converter.convert(this.readAndTrim(index));
	}

	@Override
	public Date getDateWithPattern(String name, String pattern) {
		try {
			return getDateWithPattern(indexOf(name), pattern);
		}
		catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e.getMessage() + ", name: [" + name + "]");
		}
	}

	@Override
	public Date getDateWithPattern(int index, String pattern, Date defaultValue) {
		try {
			Date d = getDateWithPattern(index, pattern);
			return (d != null) ? d : defaultValue;
		}
		catch (IllegalArgumentException e) {
			return defaultValue;
		}

	}

	@Override
	public Date getDateWithPattern(String name, String pattern, Date defaultValue) {
		try {
			return getDateWithPattern(indexOf(name), pattern, defaultValue);
		}
		catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e.getMessage() + ", name: [" + name + "]");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#getValue(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T getValue(String name, Class<T> valueClass) {
		Object value = values.get(indexOf(name));
		return convert(value, valueClass);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cloud.stream.tuple.Tuple#getValue(int, java.lang.Class)
	 */
	@Override
	public <T> T getValue(int index, Class<T> valueClass) {
		return convert(values.get(index), valueClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Tuple select(String expression) {
		EvaluationContext context = new StandardEvaluationContext(toMap());
		ExpressionParser parser = new SpelExpressionParser();
		Expression exp = parser.parseExpression(expression);

		Object result = exp.getValue(context);
		Map<String, Object> resultMap = null;
		if (ClassUtils.isAssignableValue(Map.class, result)) {
			resultMap = (Map<String, Object>) result;
		}
		if (resultMap != null) {
			return toTuple(resultMap);
		}
		else {
			return new DefaultTuple(new ArrayList<String>(0), new ArrayList<>(0),
					this.configurableConversionService);
		}
	}

	/**
	 * @return names and values as a {@code Map<String, Object>}
	 */
	Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>(values.size());
		for (int i = 0; i < values.size(); i++) {
			map.put(names.get(i), values.get(i));
		}
		return map;
	}

	Tuple toTuple(Map<String, Object> resultMap) {

		List<String> newNames = new ArrayList<>();
		List<Object> newValues = new ArrayList<>();
		for (String name : resultMap.keySet()) {
			newNames.add(name);
		}
		for (Object value : resultMap.values()) {
			newValues.add(value);
		}
		return new DefaultTuple(newNames, newValues, this.configurableConversionService);

	}

	@SuppressWarnings("unchecked")
	<T> T convert(Object value, Class<T> targetType) {
		return (T) configurableConversionService.convert(value, TypeDescriptor.forObject(value),
				TypeDescriptor.valueOf(targetType));
	}

	/**
	 * Find the index in the names collection for the given name.
	 * Returns -1 if not found.
	 */
	protected int indexOf(String name) {
		return names.indexOf(name);
	}

	/**
	 * @param tupleToStringConverter used to convert a {@link Tuple} to a String
	 */
	protected void setTupleToStringConverter(Converter<Tuple, String> tupleToStringConverter) {
		Assert.notNull(tupleToStringConverter, "tupleToStringConverter cannot be null");
		this.tupleToStringConverter = tupleToStringConverter;
	}

	@Override
	/**
	 * The format is of the form DefaultTuple [names="n1'
	 */
	public String toString() {
		return tupleToStringConverter.convert(this);
	}

	
	public ConfigurableConversionService getConversionService() {
		return this.configurableConversionService;
	}

}
