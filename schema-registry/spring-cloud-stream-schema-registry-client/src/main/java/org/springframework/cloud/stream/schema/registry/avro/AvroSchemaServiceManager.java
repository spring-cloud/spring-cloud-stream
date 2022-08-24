/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.schema.registry.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * Manage a {@link Schema} together with its String representation.
 *
 * Helps to substitute the default implementation of {@link org.apache.avro.Schema}
 * Generation using Custom Avro schema generator
 *
 * Provide a custom bean definition of {@link AvroSchemaServiceManager} and mark
 * it as @Primary to override the default implementation
 *
 * @author Ish Mahajan
 *
 */
public interface AvroSchemaServiceManager {

	/**
	 * get {@link Schema}.
	 * @param clazz {@link Class} for which schema generation is required
	 * @return returns avro schema for given class
	 */
	Schema getSchema(Class<?> clazz);

	/**
	 * get {@link DatumWriter}.
	 * @param type {@link Class} of java object which needs to be serialized
	 * @param schema {@link Schema} of object which needs to be serialized
	 * @return datum writer which can be used to write Avro payload
	 */
	DatumWriter<Object> getDatumWriter(Class<? extends Object> type, Schema schema);

	/**
	 * get {@link DatumReader}.
	 * @param type {@link Class} of java object which needs to be serialized
	 * @param schema {@link Schema} default schema of object which needs to be de-serialized
	 * @param writerSchema {@link Schema} writerSchema provided at run time
	 * @return datum reader which can be used to read Avro payload
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	DatumReader<Object> getDatumReader(Class<? extends Object> type, Schema schema, Schema writerSchema);

	/**
	 * read data from avro type payload {@link DatumReader}.
	 * @param targetClass {@link Class} of java object which needs to be serialized
	 * @param payload {@link byte} serialized payload of object which needs to be de-serialized
	 * @param readerSchema {@link Schema} readerSchema of object which needs to be de-serialized
	 * @param writerSchema {@link Schema} writerSchema used to while serializing payload
	 * @return java object after reading Avro Payload
	 * @throws IOException in case of error
	 */
	Object readData(Class<? extends Object> targetClass, byte[] payload, Schema readerSchema, Schema writerSchema)
																	throws IOException;
}
