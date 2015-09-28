/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.tuple.kryo;

import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;

/**
 * Deserializes Tuples by writing the field names and then the values as class/object pairs
 * followed by the tuple Id and timestamp.
 *
 * @author David Turanski
 */
public class DefaultTupleSerializer extends Serializer<Tuple> {
	@Override
	public void write(Kryo kryo, Output output, Tuple tuple) {
		kryo.writeObject(output, tuple.getFieldNames());
		for (Object val: tuple.getValues()) {
			kryo.writeClassAndObject(output, val);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Tuple read(Kryo kryo, Input input, Class<Tuple> type) {
		List<String> names = kryo.readObject(input, ArrayList.class);
		List<Object> values = new ArrayList<>(names.size());
		for (int i = 0; i < names.size(); i++) {
			Object val = kryo.readClassAndObject(input);
			values.add(i, val);
		}
		return TupleBuilder.tuple().ofNamesAndValues(names, values);
	}
}
