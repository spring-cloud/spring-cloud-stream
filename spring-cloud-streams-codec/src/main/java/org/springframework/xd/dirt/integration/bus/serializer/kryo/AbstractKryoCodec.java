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

package org.springframework.xd.dirt.integration.bus.serializer.kryo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.serializer.AbstractCodec;

/**
 * Base class for Codecs using {@link com.esotericsoftware.kryo.Kryo}
 *
 * @author David Turanski
 */
public abstract class AbstractKryoCodec<T> extends AbstractCodec<T> {

	private final KryoFactory factory;

	protected final KryoPool pool;

	protected AbstractKryoCodec() {
		factory = new KryoFactory() {
			public Kryo create() {
				Kryo kryo = new Kryo();
				// configure kryo instance, customize settings
				configureKryoInstance(kryo);
				return kryo;
			}
		};
		// Build pool with SoftReferences enabled (optional)
		pool = new KryoPool.Builder(factory).softReferences().build();
	}

	/**
	 * Serialize an object using an existing output stream
	 *
	 * @param object the object to be serialized
	 * @param outputStream the output stream, e.g. a FileOutputStream
	 * @throws IOException
	 */
	@Override
	public void serialize(final T object, OutputStream outputStream) throws IOException {
		Assert.notNull(outputStream, "'outputSteam' cannot be null");
		final Output output = new Output(outputStream);
		try {
			pool.run(new KryoCallback<Object>() {
				@Override
				public Object execute(Kryo kryo) {
					doSerialize(kryo, object, output);
					return Void.class;
				}
			});
		} finally {
			output.close();
		}
	}

	/**
	 * Deserialize an object when the type is known
	 *
	 * @param inputStream the input stream containing the serialized object
	 * @return the object
	 * @throws IOException
	 */
	@Override
	public T deserialize(InputStream inputStream) throws IOException {
		final Input input = new Input(inputStream);
		try {
			T result = pool.run(new KryoCallback<T>() {
				@Override
				public T execute(Kryo kryo) {
					return doDeserialize(kryo, input);
				}
			});
			return result;
		} finally {
			input.close();
		}
	}

	protected abstract void doSerialize(Kryo kryo, T object, Output output);

	protected abstract T doDeserialize(Kryo kryo, Input input);

	protected void configureKryoInstance(Kryo kryo) {
	}
}
