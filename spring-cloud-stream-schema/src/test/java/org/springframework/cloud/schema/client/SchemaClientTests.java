/*
 * Copyright 2017 the original author or authors.
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


package org.springframework.cloud.schema.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.client.CachingRegistryClient;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Vinicius Carvalho
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SchemaClientTests.BootApplicationTest.class, webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {"spring.cloud.stream.schemaRegistryClient.caching=true","debug=true"})
@SpyBean(SchemaRegistryClient.class)
public class SchemaClientTests {

	@Autowired
	private CachingRegistryClient cachingRegistryClient;

	@Mock
	private SchemaRegistryClient mock;

	@Before
	public void setup(){
		cachingRegistryClient.setDelegate(mock);
	}

	@Test
	public void testCachingEnabled(){
		Mockito.when(mock.fetch(Mockito.eq(1))).thenReturn("foo");
		Mockito.when(mock.fetch(Mockito.any(SchemaReference.class))).thenReturn("bar");

		SchemaReference registered = new SchemaReference("baz",1,"avro");
		SchemaRegistrationResponse response = new SchemaRegistrationResponse();
		response.setId(2);
		response.setSchemaReference(registered);
		Mockito.when(mock.register("baz","avro","bar")).thenReturn(response);

		String result = cachingRegistryClient.fetch(1);
		Assert.assertEquals("foo",result);
		cachingRegistryClient.fetch(1);
		Mockito.verify(mock,Mockito.times(1)).fetch(1);

		SchemaReference ref = new SchemaReference("foo",1,"avro");
		String refResult = cachingRegistryClient.fetch(ref);
		Assert.assertEquals("bar",refResult);
		cachingRegistryClient.fetch(ref);
		Mockito.verify(mock, Mockito.times(1)).fetch(ref);


		cachingRegistryClient.register("baz","avro","bar");
		cachingRegistryClient.fetch(2);
		Mockito.verify(mock,Mockito.times(1)).fetch(1);
	}

	@SpringBootApplication(scanBasePackages = "org.springframework.cloud.stream.schema")
	static class BootApplicationTest {

		public static void main(String[] args) {
			SpringApplication.run(BootApplicationTest.class,args);
		}



	}

}
