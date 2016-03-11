/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.kafka.support.LoggingProducerListener;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.util.ObjectUtils;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({KryoCodecAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class})
@EnableConfigurationProperties({KafkaBinderConfigurationProperties.class})
@PropertySource("classpath:/META-INF/spring-cloud-stream/kafka-binder.properties")
public class KafkaBinderConfiguration {

	@Autowired
	private Codec codec;

	@Autowired
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Autowired
	private ProducerListener producerListener;

	@Bean
	ZookeeperConnect zookeeperConnect() {
		ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
		zookeeperConnect.setZkConnect(kafkaBinderConfigurationProperties.getZkConnectionString());
		return zookeeperConnect;
	}

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder() {
		String[] headers = kafkaBinderConfigurationProperties.getHeaders();
		String kafkaConnectionString = kafkaBinderConfigurationProperties.getKafkaConnectionString();
		String zkConnectionString = kafkaBinderConfigurationProperties.getZkConnectionString();
		KafkaMessageChannelBinder kafkaMessageChannelBinder = ObjectUtils.isEmpty(headers) ?
				new KafkaMessageChannelBinder(zookeeperConnect(), kafkaConnectionString, zkConnectionString)
				: new KafkaMessageChannelBinder(zookeeperConnect(), kafkaConnectionString, zkConnectionString,
				headers);
		kafkaMessageChannelBinder.setCodec(codec);
		kafkaMessageChannelBinder.setOffsetUpdateTimeWindow(kafkaBinderConfigurationProperties.getOffsetUpdateTimeWindow());
		kafkaMessageChannelBinder.setOffsetUpdateCount(kafkaBinderConfigurationProperties.getOffsetUpdateCount());
		kafkaMessageChannelBinder.setOffsetUpdateShutdownTimeout(kafkaBinderConfigurationProperties.getOffsetUpdateShutdownTimeout());

		kafkaMessageChannelBinder.setZkSessionTimeout(kafkaBinderConfigurationProperties.getZkSessionTimeout());
		kafkaMessageChannelBinder.setZkConnectionTimeout(kafkaBinderConfigurationProperties.getZkConnectionTimeout());

		kafkaMessageChannelBinder.setFetchSize(kafkaBinderConfigurationProperties.getFetchSize());
		kafkaMessageChannelBinder.setDefaultMinPartitionCount(kafkaBinderConfigurationProperties.getMinPartitionCount());
		kafkaMessageChannelBinder.setQueueSize(kafkaBinderConfigurationProperties.getQueueSize());
		kafkaMessageChannelBinder.setReplicationFactor(kafkaBinderConfigurationProperties.getReplicationFactor());
		kafkaMessageChannelBinder.setRequiredAcks(kafkaBinderConfigurationProperties.getRequiredAcks());
		kafkaMessageChannelBinder.setMaxWait(kafkaBinderConfigurationProperties.getMaxWait());

		kafkaMessageChannelBinder.setProducerListener(producerListener);
		return kafkaMessageChannelBinder;
	}

	@Bean
	@ConditionalOnMissingBean(ProducerListener.class)
	ProducerListener producerListener() {
		return new LoggingProducerListener();
	}

	@Bean
	KafkaBinderHealthIndicator healthIndicator(KafkaMessageChannelBinder kafkaMessageChannelBinder) {
		return new KafkaBinderHealthIndicator(kafkaMessageChannelBinder);
	}
}
