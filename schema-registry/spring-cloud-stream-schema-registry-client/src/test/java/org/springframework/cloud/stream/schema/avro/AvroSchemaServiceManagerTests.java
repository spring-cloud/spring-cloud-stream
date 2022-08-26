/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro;


import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.schema.avro.domain.FoodOrder;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManager;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Ish Mahajan
 */
public class AvroSchemaServiceManagerTests {

	private final Log logger = LogFactory.getLog(AvroSchemaServiceManagerTests.class);

	@Test
	public void testWithDefaultImplementation() throws IOException {
		assertThatThrownBy(() -> {

			AvroSchemaServiceManager defaultServiceManager = new AvroSchemaServiceManagerImpl();

			Schema schema = defaultServiceManager.getSchema(FoodOrder.class);
			FoodOrder foodOrder = new FoodOrder();
			foodOrder.setRestaurant("Spring Kitchen");
			foodOrder.setOrderDescription("avro makhani");
			foodOrder.setCustomerAddress("world wide web");
			File file = new File("target/foodorder.avro");

			DatumWriter datumWriter = defaultServiceManager.getDatumWriter(foodOrder.getClass(), schema);
			DataFileWriter<FoodOrder> dataFileWriter = new DataFileWriter<FoodOrder>(datumWriter);
			dataFileWriter.create(schema, file);
			dataFileWriter.append(foodOrder);

			FoodOrder foodOrder2 = new FoodOrder();
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
				logger.info("De-serialised Successfully : " + foodOrderDeserialized);
			}
		}).isInstanceOf(DataFileWriter.AppendWriteException.class);
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
					fail("Error while setting acceptJsonFormatVisitor {}", e);
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
					fail("Error while setting acceptJsonFormatVisitor {}", e);
				}
				return mapper.readerFor(targetClass)
						.with(new AvroSchema(readerSchema))
						.readValue(payload);
			}
		};

		FoodOrder foodOrder1 = new FoodOrder();
		foodOrder1.setRestaurant("Spring Kitchen");
		foodOrder1.setOrderDescription("avro makhani");
		foodOrder1.setCustomerAddress("world wide web");
		FoodOrder foodOrder2 = new FoodOrder();
		foodOrder2.setRestaurant("Spring Kitchen");

		Schema schema = manager.getSchema(FoodOrder.class);
		AvroMapper mapper = new AvroMapper();
		byte[] payload1 = mapper.writer(new AvroSchema(schema)).writeValueAsBytes(foodOrder1);
		byte[] payload2 = mapper.writer(new AvroSchema(schema)).writeValueAsBytes(foodOrder2);
		foodOrder1 = (FoodOrder) manager.readData(foodOrder1.getClass(), payload1, schema, schema);
		foodOrder2 = (FoodOrder) manager.readData(foodOrder1.getClass(), payload2, schema, schema);
		assertThat(foodOrder2.getOrderDescription()).isNull();
		assertThat(foodOrder2.getCustomerAddress()).isNull();
	}

	@Test
	public void testAvroSchemaMessageConverter() {
		AvroSchemaMessageConverter converter = new AvroSchemaMessageConverter();
		MimeType mimeType = new MimeType("application", "avro");
		assertThat(mimeType).isEqualTo(converter.getSupportedMimeTypes().get(0));

		AvroSchemaMessageConverter converter2 = new AvroSchemaMessageConverter(mimeType);
		assertThat(mimeType).isEqualTo(converter2.getSupportedMimeTypes().get(0));

		AvroSchemaMessageConverter converter3 =
				new AvroSchemaMessageConverter(Lists.newArrayList(mimeType));
		assertThat(mimeType).isEqualTo(converter3.getSupportedMimeTypes().get(0));

		AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();
		AvroSchemaMessageConverter converter4 = new AvroSchemaMessageConverter(manager);
		assertThat(mimeType).isEqualTo(converter4.getSupportedMimeTypes().get(0));

		AvroSchemaMessageConverter converter5 =
				new AvroSchemaMessageConverter(Lists.newArrayList(mimeType), manager);
		Schema schema = manager.getSchema(FoodOrder.class);
		converter5.setSchema(schema);
		assertThat(mimeType).isEqualTo(converter5.getSupportedMimeTypes().get(0));
		assertThat(schema).isEqualTo(converter5.getSchema());
	}

	@Test
	public void testAvroSchemaMessageConverterException() {
		assertThatThrownBy(() -> {
			MimeType mimeType = new MimeType("application", "avro");
			AvroSchemaServiceManager manager = new AvroSchemaServiceManagerImpl();
			AvroSchemaMessageConverter converter =
					new AvroSchemaMessageConverter(Lists.newArrayList(mimeType), manager);
			converter.setSchemaLocation(new ByteArrayResource(new byte[2]) {
			});
		}).isInstanceOf(SchemaParseException.class);
	}
}
