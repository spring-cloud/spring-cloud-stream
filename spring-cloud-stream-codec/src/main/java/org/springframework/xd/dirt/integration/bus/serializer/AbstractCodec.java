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

package org.springframework.xd.dirt.integration.bus.serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.springframework.core.serializer.Deserializer;
import org.springframework.core.serializer.Serializer;


/**
 * Support class providing convenience methods for codecs.
 * 
 * @author David Turanski
 */
public abstract class AbstractCodec<T> implements Serializer<T>, Deserializer<T> {

	/**
	 * Deserialize a byte array.
	 * 
	 * @param bytes
	 * @throws IOException
	 */
	public T deserialize(byte[] bytes) throws IOException {
		return deserialize(new ByteArrayInputStream(bytes));
	}
}
