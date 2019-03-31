/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.schema.avro;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.cloud.schema.avro.domain.FoodOrder;
import org.springframework.cloud.stream.schema.avro.AvroSchemaServiceManager;
import org.springframework.cloud.stream.schema.avro.AvroSchemaServiceManagerImpl;

/**
 * @author 5aab
 */
@Slf4j
public class AvroSchemaServiceManagerTests {

	@Test(expected = DataFileWriter.AppendWriteException.class)
	public void testWithDefaultImplementation() throws IOException {
		AvroSchemaServiceManager defaultServiceManager = new AvroSchemaServiceManagerImpl();
		Schema schema = defaultServiceManager.getSchema(FoodOrder.class);
		FoodOrder foodOrder = FoodOrder.builder().restaurant("Spring Kitchen")
			.orderDescription("avro makhani").customerAddress("world wide web").build();
		File file = new File("foodorder.avro");
		DatumWriter datumWriter = defaultServiceManager.getDatumWriter(foodOrder.getClass(), schema);
		DataFileWriter<FoodOrder> dataFileWriter = new DataFileWriter<FoodOrder>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(foodOrder);

		FoodOrder foodOrder2 = FoodOrder.builder().restaurant(null)
			.orderDescription(null).customerAddress(null).build();
		dataFileWriter.append(foodOrder2);
		dataFileWriter.close();

		DatumReader userDatumReader = defaultServiceManager.getDatumReader(foodOrder.getClass(), schema, schema);
		DataFileReader<FoodOrder> dataFileReader = new DataFileReader<FoodOrder>(file, userDatumReader);
		FoodOrder foodOrderDeserialized = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			foodOrderDeserialized = dataFileReader.next(foodOrderDeserialized);
			System.out.println("De-serialised Successfully : " + foodOrderDeserialized);
		}
	}

	@Test
	public void testWithCustomImplementation() throws IOException {
		AvroSchemaServiceManager manager = new AvroSchemaServiceManager() {
			@Override
			public Schema getSchema(Class<?> clazz) {
				ObjectMapper mapper = new ObjectMapper(new AvroFactory());
				AvroSchemaGenerator gen = new AvroSchemaGenerator();
				try {
					mapper.acceptJsonFormatVisitor(FoodOrder.class, gen);
				}
				catch (JsonMappingException e) {
					log.error("Error while setting acceptJsonFormatVisitor {}", e);
				}
				AvroSchema schemaWrapper = gen.getGeneratedSchema();
				return schemaWrapper.getAvroSchema();
			}

			@Override
			public DatumWriter<Object> getDatumWriter(Class<?> type, Schema schema) {
				return new AvroSchemaServiceManagerImpl().getDatumWriter(type, schema);
			}

			@Override
			public DatumReader<Object> getDatumReader(Class<?> type, Schema schema, Schema writerSchema) {
				return new AvroSchemaServiceManagerImpl().getDatumReader(type, schema, schema);
			}

			@Override
			public Object readData(Class<? extends Object> targetClass, byte[] payload, Schema readerSchema,
											Schema writerSchema) throws IOException {
				ObjectMapper mapper = new ObjectMapper(new AvroFactory());
				AvroSchemaGenerator gen = new AvroSchemaGenerator();
				try {
					mapper.acceptJsonFormatVisitor(targetClass, gen);
				}
				catch (JsonMappingException e) {
					log.error("Error while setting acceptJsonFormatVisitor {}", e);
				}
				return mapper.readerFor(targetClass)
					.with(new AvroSchema(readerSchema))
					.readValue(payload);
			}
		};

		FoodOrder foodOrder1 = FoodOrder.builder().restaurant("Spring Kitchen")
			.orderDescription("avro makhani").customerAddress("world wide web").build();
		FoodOrder foodOrder2 = FoodOrder.builder().restaurant("Spring Kitchen")
			.orderDescription(null).customerAddress(null).build();
		Schema schema = manager.getSchema(FoodOrder.class);
		AvroMapper mapper = new AvroMapper();
		byte[] payload1 = mapper.writer(new AvroSchema(schema)).writeValueAsBytes(foodOrder1);
		byte[] payload2 = mapper.writer(new AvroSchema(schema)).writeValueAsBytes(foodOrder2);
		foodOrder1 = (FoodOrder) manager.readData(foodOrder1.getClass(), payload1, schema, schema);
		foodOrder2 = (FoodOrder) manager.readData(foodOrder1.getClass(), payload2, schema, schema);
		Assert.assertNull(foodOrder2.getOrderDescription());
		Assert.assertNull(foodOrder2.getCustomerAddress());
	}
}
