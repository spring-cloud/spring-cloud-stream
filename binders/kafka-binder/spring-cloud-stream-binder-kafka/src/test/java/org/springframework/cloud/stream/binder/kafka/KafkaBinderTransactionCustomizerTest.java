/*
 * Copyright 2025-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.retry.support.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(count = 1, controlledShutdown = true, brokerProperties = {"transaction.state.log.replication.factor=1",
	"transaction.state.log.min.isr=1"})
class KafkaBinderTransactionCustomizerTest {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@SuppressWarnings("unchecked")
	@Test
	void clientFactoryCustomizerAppliedBeforeTransactionManager() throws Exception {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
			.singletonList(embeddedKafka.getBrokersAsString()));

		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
			kafkaProperties, mock(ObjectProvider.class));
		configurationProperties.getTransaction().setTransactionIdPrefix("custom-tx-");

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
			configurationProperties, kafkaProperties, prop -> {
		});
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate());

		// Create a tracking list for customized factories
		List<ProducerFactory<?, ?>> customizedFactories = new ArrayList<>();

		// Create a customizer that we'll register after the binder is created
		ClientFactoryCustomizer customizer = new ClientFactoryCustomizer() {
			@Override
			public void configure(ProducerFactory<?, ?> pf) {
				customizedFactories.add(pf);
			}
		};

		KafkaMessageChannelBinder binder = spy(new KafkaMessageChannelBinder(
			configurationProperties, provisioningProvider));

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		binder.setApplicationContext(applicationContext);

		// Add the customizer AFTER binder creation but BEFORE afterPropertiesSet
		binder.addClientFactoryCustomizer(customizer);

		// Now initialize the binder (this triggers onInit)
		binder.afterPropertiesSet();

		// Verify KafkaMessageChannelBinder.getProducerFactory was called from onInit
		verify(binder).getProducerFactory(
			eq("custom-tx-"),
			any(ExtendedProducerProperties.class),
			eq("custom-tx-.producer"),
			isNull());

		// Verify customizer was applied
		assertThat(customizedFactories).isNotEmpty();

		// Verify that the producer factory from the transaction manager is in our list of customized factories
		KafkaTransactionManager<?, ?> txManager = (KafkaTransactionManager<?, ?>)
			TestUtils.getPropertyValue(binder, "transactionManager");
		assertThat(txManager).isNotNull();
		ProducerFactory<?, ?> producerFactory = txManager.getProducerFactory();
		// This verifies that the same producer factory that was customized is used for the transaction manager
		assertThat(customizedFactories).contains(producerFactory);
	}

	@SuppressWarnings("unchecked")
	@Test
	void multipleCustomizersAppliedInOrder() throws Exception {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
			.singletonList(embeddedKafka.getBrokersAsString()));

		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
			kafkaProperties, mock(ObjectProvider.class));
		configurationProperties.getTransaction().setTransactionIdPrefix("multi-tx-");

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
			configurationProperties, kafkaProperties, prop -> {
		});
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate());

		// Track order of customizers and customized factories
		List<String> customizationOrder = new ArrayList<>();
		List<ProducerFactory<?, ?>> customizedFactories = new ArrayList<>();

		KafkaMessageChannelBinder binder = spy(new KafkaMessageChannelBinder(
			configurationProperties, provisioningProvider));

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		binder.setApplicationContext(applicationContext);

		// Add multiple customizers
		binder.addClientFactoryCustomizer(new ClientFactoryCustomizer() {
			@Override
			public void configure(ProducerFactory<?, ?> pf) {
				customizationOrder.add("customizer1");
				customizedFactories.add(pf);
			}
		});

		binder.addClientFactoryCustomizer(new ClientFactoryCustomizer() {
			@Override
			public void configure(ProducerFactory<?, ?> pf) {
				customizationOrder.add("customizer2");
			}
		});

		binder.addClientFactoryCustomizer(new ClientFactoryCustomizer() {
			@Override
			public void configure(ProducerFactory<?, ?> pf) {
				customizationOrder.add("customizer3");
			}
		});

		binder.afterPropertiesSet();

		assertThat(customizationOrder).containsExactly("customizer1", "customizer2", "customizer3");

		KafkaTransactionManager<?, ?> txManager = (KafkaTransactionManager<?, ?>)
			TestUtils.getPropertyValue(binder, "transactionManager");
		assertThat(txManager).isNotNull();
		ProducerFactory<?, ?> producerFactory = txManager.getProducerFactory();
		// Verify that the producer factory used in transaction manager is one that was customized
		assertThat(customizedFactories).contains(producerFactory);
	}

}
