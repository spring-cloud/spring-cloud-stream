/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


public interface AvroSchemaServiceManager {
	Schema getSchema(Class<?> clazz);

	DatumWriter<Object> getDatumWriter(Class<Object> type, Schema schema);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	DatumReader<Object> getDatumReader(Class<Object> type, Schema schema, Schema writerSchema);

	Object readData(Class<?> targetClass, byte[] payload, Schema readerSchema, Schema writerSchema) throws IOException;
}
