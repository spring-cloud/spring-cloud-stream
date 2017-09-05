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
		Message<?> message = MessageBuilder.withPayload("foo").build();
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
		Message message = MessageBuilder.withPayload("foo").build();
		Object result = kryoMessageConverter.fromMessage(message,String.class);
	}

	@Test(expected = MessageConversionException.class)
	public void readWithWrongPayloadFormat() throws Exception{
		KryoMessageConverter kryoMessageConverter = new KryoMessageConverter(null,true);
		Message message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,KryoMessageConverter.KRYO_MIME_TYPE+";type=java.lang.String").build();
		Object result = kryoMessageConverter.fromMessage(message,String.class);
	}

}
