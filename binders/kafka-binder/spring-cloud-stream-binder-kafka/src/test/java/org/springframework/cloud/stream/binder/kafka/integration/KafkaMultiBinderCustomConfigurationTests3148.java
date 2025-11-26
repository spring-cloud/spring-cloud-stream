/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaBindingRebalanceListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 *
 */
@SpringBootTest(classes = KafkaMultiBinderCustomConfigurationTests3148.SampleApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@EmbeddedKafka(controlledShutdown = true)
public class KafkaMultiBinderCustomConfigurationTests3148 {

	@Autowired
	ApplicationContext applicationContext;

	@Autowired
	private DefaultBinderFactory binderFactory;

	@Test
	public void test() {
		ConfigurableApplicationContext kafkaContext = getBinderContext("test-binder");

		KafkaBindingRebalanceListener kafkaBindingRebalanceListener = kafkaContext.getBean(KafkaBindingRebalanceListener.class);
		assertThat(kafkaBindingRebalanceListener).isNotNull();
	}

	private ConfigurableApplicationContext getBinderContext(String binderName) {
		Field binderInstanceCacheField = ReflectionUtils.findField(DefaultBinderFactory.class, "binderInstanceCache");
		assertThat(binderInstanceCacheField).isNotNull();
		ReflectionUtils.makeAccessible(binderInstanceCacheField);
		try {
			Map<String, Map.Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>> binderInstanceCache =
				(Map<String, Map.Entry<Binder<?, ?, ?>, ConfigurableApplicationContext>>) binderInstanceCacheField.get(this.binderFactory);
			return binderInstanceCache.get(binderName).getValue();
		}
		catch (Exception e) {
			fail();
		}
		return null;
	}
	@Configuration
	@EnableAutoConfiguration
	@PropertySource(value = "classpath:test3148.yml", factory = KafkaMultiBinderCustomConfigurationTests3148.YamlPropertySourceFactory.class)
	public static class SampleApplication {

		@Bean
		public Consumer<String> testConsumer() {
			return consumer -> {

			};
		}

		@Bean
		public KafkaBindingRebalanceListener rebalanceListener() {
			return new KafkaBindingRebalanceListener() {
				public void onPartitionsAssigned(String bindingName, org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, Collection<TopicPartition> partitions, boolean initial) {
					// do nothing
				}
			};
		}
	}



	public static class YamlPropertySourceFactory implements PropertySourceFactory {

		@Override
		public org.springframework.core.env.PropertySource<?> createPropertySource(String name, EncodedResource encodedResource)
			throws IOException {
			YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
			factory.setResources(encodedResource.getResource());

			Properties properties = factory.getObject();

			return new PropertiesPropertySource(encodedResource.getResource().getFilename(), properties);
		}
	}
}
