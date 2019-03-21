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

package org.springframework.cloud.stream.config;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = InboundJsonToTupleConversionTest.FooProcessor.class)
public class InboundJsonToTupleConversionTest {

	@Autowired
	private Processor testProcessor;

	@Autowired
	private BinderFactory binderFactory;

	@Test
	public void testInboundJsonTupleConversion() throws Exception {
		this.testProcessor.input()
				.send(MessageBuilder.withPayload("{'name':'foo'}").build());
		@SuppressWarnings("unchecked")
		Message<byte[]> received = (Message<byte[]>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		String payload = new String(received.getPayload(), StandardCharsets.UTF_8);
		assertThat(TupleBuilder.fromString(payload))
				.isEqualTo(TupleBuilder.tuple().of("name", "foo"));
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/inboundjsontuple/inbound-json-tuple.properties")
	public static class FooProcessor {

		@ServiceActivator(inputChannel = "input", outputChannel = "output")
		public Tuple consume(Tuple tuple) {
			return tuple;
		}

	}

}
