/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Field;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { KafkaBinderConfiguration.class, KafkaAutoConfiguration.class,
		KafkaBinderConfigurationTest.class })
public class KafkaBinderConfigurationTest {

	@Autowired
	private KafkaMessageChannelBinder kafkaMessageChannelBinder;

	@Test
	public void testKafkaBinderProducerListener() {
		assertThat(this.kafkaMessageChannelBinder).isNotNull();
		Field producerListenerField = ReflectionUtils.findField(
				KafkaMessageChannelBinder.class, "producerListener",
				ProducerListener.class);
		ReflectionUtils.makeAccessible(producerListenerField);
		ProducerListener producerListener = (ProducerListener) ReflectionUtils
				.getField(producerListenerField, this.kafkaMessageChannelBinder);
		assertThat(producerListener).isNotNull();
	}

}
