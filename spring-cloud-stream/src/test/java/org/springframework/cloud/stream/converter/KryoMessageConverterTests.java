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

package org.springframework.cloud.stream.converter;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;

/**
 * @author Vinicius Carvalho
 */
public class KryoMessageConverterTests {

	@Test
	public void convertStringType() throws Exception {
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,"application/x-java-object").build();
		Message converted = kryoMessageConverter.toMessage(message.getPayload(),message.getHeaders());
		Assert.assertNotNull(converted);
		Assert.assertEquals("application/x-java-object;type=java.lang.String",converted.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString());
	}

	@Test
	public void readStringType() throws Exception {
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Kryo kryo = new Kryo();
		String foo = "foo";
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		kryo.writeObject(output,foo);
		output.close();
		Message message = MessageBuilder.withPayload(baos.toByteArray()).setHeader(MessageHeaders.CONTENT_TYPE,KryoMessageConverter.KRYO_MIME_TYPE+";type=java.lang.String").build();
		Object result = kryoMessageConverter.fromMessage(message,String.class);
		Assert.assertEquals(foo,result);
	}

	@Test
	public void testMissingHeaders() throws Exception {
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Kryo kryo = new Kryo();
		String foo = "foo";
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		kryo.writeObject(output,foo);
		output.close();
		Message message = MessageBuilder.withPayload(baos.toByteArray()).build();
		Object result = kryoMessageConverter.fromMessage(message,String.class);
		Assert.assertNull(result);
	}

	@Test(expected = MessageConversionException.class)
	public void readWithWrongPayloadType() throws Exception{
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Message message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,KryoMessageConverter.KRYO_MIME_TYPE+";type=java.lang.String").build();
		kryoMessageConverter.fromMessage(message,String.class);
	}

	@Test(expected = MessageConversionException.class)
	public void readWithWrongPayloadFormat() throws Exception{
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Message message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,KryoMessageConverter.KRYO_MIME_TYPE+";type=java.lang.String").build();
		kryoMessageConverter.fromMessage(message,String.class);
	}

}
