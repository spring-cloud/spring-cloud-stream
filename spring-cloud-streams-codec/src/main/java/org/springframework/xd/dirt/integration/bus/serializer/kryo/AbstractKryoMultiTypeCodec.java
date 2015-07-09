/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.bus.serializer.kryo;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;

import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoCallback;

/**
 * Base class for Codecs using {@link com.esotericsoftware.kryo.Kryo} to serialize arbitrary types
 *
 * @author David Turanski
 * @since 1.0
 */
abstract class AbstractKryoMultiTypeCodec<T> extends AbstractKryoCodec<T> implements MultiTypeCodec<T> {

	/**
	 * Deserialize an object of a given type given a byte array
	 *
	 * @param bytes the byte array containing the serialized object
	 * @param type the object's class
	 * @return the object
	 * @throws IOException
	 */
	@Override
	public T deserialize(byte[] bytes, Class<? extends T> type) throws IOException {
		final Input input = new Input(bytes);
		try {
			return deserialize(input, type);
		} finally {
			input.close();
		}
	}

	/**
	 * Deserialize an object of a given type given an InputStream
	 *
	 * @param inputStream the input stream containing the serialized object
	 * @param type the object's class
	 * @return the object
	 * @throws IOException
	 */
	@Override
	public T deserialize(InputStream inputStream, final Class<? extends T> type) throws IOException {
		final Input input = new Input(inputStream);
		try {
			return deserialize(input, type);
		} finally {
			input.close();
		}
	}

	/**
	 * Deserialize an object of a given type given an Input.
	 *
	 * @param input the Kryo input stream containing the serialized object
	 * @param type the object's class
	 * @return the object
	 * @throws IOException
	 */
	protected T deserialize(final Input input, final Class<? extends T> type) throws IOException {
		return pool.run(new KryoCallback<T>() {

			@Override
			public T execute(Kryo kryo) {
				return doDeserialize(kryo, input, type);
			}
		});
	}

	/**
	 * Infers the type from this class's generic type argument
	 * @param kryo the Kryo
	 * @param input the input
	 * @return the object
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected T doDeserialize(Kryo kryo, Input input) {
		Class<T> type = (Class<T>) (
				(ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		return doDeserialize(kryo, input, type);
	}

	protected abstract T doDeserialize(Kryo kryo, Input input, Class<? extends T> type);

}
