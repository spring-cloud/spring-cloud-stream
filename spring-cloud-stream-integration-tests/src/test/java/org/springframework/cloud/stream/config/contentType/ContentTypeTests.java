/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.config.contentType;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.converter.KryoMessageConverter;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@SuppressWarnings("unchecked")
public class ContentTypeTests {

	private ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testSendWithDefaultContentType() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false")) {

			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			User user = new User("Alice");
			source.output().send(MessageBuilder.withPayload(user).build());
			Message<String> message = (Message<String>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			User received = this.mapper.readValue(message.getPayload(), User.class);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.APPLICATION_JSON));
			assertThat(user.getName()).isEqualTo(received.getName());
		}
	}

	@Test
	public void testSendJsonAsString() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			User user = new User("Alice");
			String json = this.mapper.writeValueAsString(user);
			source.output().send(MessageBuilder.withPayload(user).build());
			Message<String> message = (Message<String>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.APPLICATION_JSON));
			assertThat(json).isEqualTo(message.getPayload());
		}
	}

	@Test
	public void testSendJsonString() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			source.output().send(MessageBuilder.withPayload("foo").build());
			Message<String> message = (Message<String>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.APPLICATION_JSON));
			assertThat("foo").isEqualTo(message.getPayload());
		}
	}

	@Test
	public void testSendBynaryData() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false")) {

			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			byte[] data = new byte[] { 0, 1, 2, 3 };
			source.output()
					.send(MessageBuilder.withPayload(data)
							.setHeader(MessageHeaders.CONTENT_TYPE,
									MimeTypeUtils.APPLICATION_OCTET_STREAM)
							.build());
			Message<byte[]> message = (Message<byte[]>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.APPLICATION_OCTET_STREAM));
			assertThat(message.getPayload()).isEqualTo(data);
		}
	}

	@Test
	public void testSendBinaryDataWithContentType() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=image/jpeg")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			byte[] data = new byte[] { 0, 1, 2, 3 };
			source.output().send(MessageBuilder.withPayload(data).build());
			Message<byte[]> message = (Message<byte[]>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(message.getPayload()).isEqualTo(data);
		}
	}

	@Test
	public void testSendBinaryDataWithContentTypeUsingHeaders() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0",
				"--spring.jmx.enabled=false")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			byte[] data = new byte[] { 0, 1, 2, 3 };
			source.output().send(MessageBuilder.withPayload(data)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.IMAGE_JPEG)
					.build());
			Message<byte[]> message = (Message<byte[]>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.IMAGE_JPEG));
			assertThat(message.getPayload()).isEqualTo(data);
		}
	}

	@Test
	public void testSendJavaSerializable() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=application/x-java-serialized-object")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			User user = new User("Alice");
			source.output().send(MessageBuilder.withPayload(user).build());
			Message<User> message = (Message<User>) collector.forChannel(source.output())
					.poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT));
			User received = message.getPayload();
			assertThat(user.getName()).isEqualTo(received.getName());
		}
	}

	@Test
	public void testSendKryoSerialized() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=application/x-java-object")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			User user = new User("Alice");
			source.output().send(MessageBuilder.withPayload(user).build());
			Message<User> message = (Message<User>) collector.forChannel(source.output())
					.poll(1, TimeUnit.SECONDS);
			User received = message.getPayload();
			assertThat(message.getHeaders()
					.get(MessageHeaders.CONTENT_TYPE, MimeType.class)
					.includes(MimeType.valueOf(KryoMessageConverter.KRYO_MIME_TYPE)));
			assertThat(user.getName()).isEqualTo(received.getName());

		}
	}

	@Test
	public void testSendStringType() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=text/plain")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			User user = new User("Alice");
			source.output().send(MessageBuilder.withPayload(user).build());
			Message<String> message = (Message<String>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MimeTypeUtils.TEXT_PLAIN));
			assertThat(message.getPayload()).isEqualTo(user.toString());
		}
	}

	@Test
	public void testSendTuple() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SourceApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.output.contentType=application/x-spring-tuple")) {
			MessageCollector collector = context.getBean(MessageCollector.class);
			Source source = context.getBean(Source.class);
			Tuple tuple = TupleBuilder.tuple().of("foo", "bar");
			source.output().send(MessageBuilder.withPayload(tuple).build());
			Message<byte[]> message = (Message<byte[]>) collector
					.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertThat(
					message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
							.includes(MessageConverterUtils.X_SPRING_TUPLE));
			assertThat(TupleBuilder.fromString(new String(message.getPayload())))
					.isEqualTo(tuple);
		}
	}

	@Test
	public void testReceiveWithDefaults() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SinkApplication.class, "--server.port=0", "--spring.jmx.enabled=false")) {
			TestSink testSink = context.getBean(TestSink.class);
			SinkApplication sourceApp = context.getBean(SinkApplication.class);
			User user = new User("Alice");
			testSink.pojo().send(MessageBuilder
					.withPayload(this.mapper.writeValueAsBytes(user)).build());
			Map<String, Object> headers = (Map<String, Object>) sourceApp.arguments.pop();
			User received = (User) sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MimeTypeUtils.APPLICATION_JSON));
			assertThat(user.getName()).isEqualTo(received.getName());
		}
	}

	@Test
	public void testReceiveRawWithDifferentContentTypes() {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SinkApplication.class, "--server.port=0", "--spring.jmx.enabled=false")) {
			TestSink testSink = context.getBean(TestSink.class);
			SinkApplication sourceApp = context.getBean(SinkApplication.class);
			testSink.raw().send(MessageBuilder.withPayload(new byte[4])
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.IMAGE_JPEG)
					.build());
			testSink.raw().send(MessageBuilder.withPayload(new byte[4])
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.IMAGE_GIF)
					.build());
			Map<String, Object> headers = (Map<String, Object>) sourceApp.arguments.pop();
			sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MimeTypeUtils.IMAGE_GIF));
			headers = (Map<String, Object>) sourceApp.arguments.pop();
			sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MimeTypeUtils.IMAGE_JPEG));
		}
	}

	@Test
	@Ignore
	public void testReceiveKryoPayload() {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SinkApplication.class, "--server.port=0", "--debug",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.pojo_input.contentType="
						+ "application/x-java-object;type=org.springframework.cloud.stream.config.contentType.User")) {
			TestSink testSink = context.getBean(TestSink.class);
			SinkApplication sourceApp = context.getBean(SinkApplication.class);
			Kryo kryo = new Kryo();
			User user = new User("Alice");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output output = new Output(baos);
			kryo.writeObject(output, user);
			output.close();
			testSink.pojo().send(MessageBuilder.withPayload(baos.toByteArray()).build());
			Map<String, Object> headers = (Map<String, Object>) sourceApp.arguments.pop();
			User received = (User) sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MimeType.valueOf(KryoMessageConverter.KRYO_MIME_TYPE)));
			assertThat(user.getName()).isEqualTo(received.getName());
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testReceiveKryoWithHeadersOverridingDefault() {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SinkApplication.class, "--server.port=0", "--spring.jmx.enabled=false")) {
			TestSink testSink = context.getBean(TestSink.class);
			SinkApplication sourceApp = context.getBean(SinkApplication.class);
			Kryo kryo = new Kryo();
			User user = new User("Alice");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output output = new Output(baos);
			kryo.writeObject(output, user);
			output.close();
			testSink.pojo()
					.send(MessageBuilder.withPayload(baos.toByteArray())
							.setHeader(MessageHeaders.CONTENT_TYPE,
									MimeType.valueOf(KryoMessageConverter.KRYO_MIME_TYPE))
							.build());
			Map<String, Object> headers = (Map<String, Object>) sourceApp.arguments.pop();
			User received = (User) sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MimeType.valueOf(KryoMessageConverter.KRYO_MIME_TYPE)));
			assertThat(user.getName()).isEqualTo(received.getName());
		}
	}

	@Test
	@Ignore
	public void testReceiveJavaSerializable() throws Exception {
		try (ConfigurableApplicationContext context = SpringApplication.run(
				SinkApplication.class, "--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.pojo_input.contentType=application/x-java-serialized-object")) {
			TestSink testSink = context.getBean(TestSink.class);
			SinkApplication sourceApp = context.getBean(SinkApplication.class);
			User user = new User("Alice");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			new ObjectOutputStream(baos).writeObject(user);
			testSink.pojo().send(MessageBuilder.withPayload(baos.toByteArray()).build());
			Map<String, Object> headers = (Map<String, Object>) sourceApp.arguments.pop();
			User received = (User) sourceApp.arguments.pop();
			assertThat(((MimeType) headers.get(MessageHeaders.CONTENT_TYPE))
					.includes(MessageConverterUtils.X_JAVA_SERIALIZED_OBJECT));
			assertThat(user.getName()).isEqualTo(received.getName());

		}
	}

	public interface TestSink {

		@Input("POJO_INPUT")
		SubscribableChannel pojo();

		@Input("STRING_INPUT")
		SubscribableChannel string();

		@Input("TUPLE_INPUT")
		SubscribableChannel tuple();

		@Input("RAW_INPUT")
		SubscribableChannel raw();

	}

	@EnableBinding(Source.class)
	@SpringBootApplication
	public static class SourceApplication {

	}

	@EnableBinding(TestSink.class)
	@SpringBootApplication
	public static class SinkApplication {

		public LinkedList<? super Object> arguments = new LinkedList<>();

		@StreamListener("POJO_INPUT")
		public void receive(User user, @Headers Map<String, Object> headers) {
			this.arguments.push(user);
			this.arguments.push(headers);
		}

		@StreamListener("TUPLE_INPUT")
		public void receive(Tuple tuple) {
		}

		@StreamListener("STRING_INPUT")
		public void receive(String string) {
		}

		@StreamListener("RAW_INPUT")
		public void receive(byte[] data, @Headers Map<String, Object> headers) {
			this.arguments.push(data);
			this.arguments.push(headers);
		}

	}

}
