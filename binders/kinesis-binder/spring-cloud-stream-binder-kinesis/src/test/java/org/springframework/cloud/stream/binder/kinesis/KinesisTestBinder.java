/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisConsumerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisProducerProperties;
import org.springframework.cloud.stream.binder.kinesis.provisioning.KinesisStreamProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageProducer;

/**
 * An {@link AbstractTestBinder} implementation for the {@link KinesisMessageChannelBinder}.
 *
 * @author Artem Bilan
 * @author Arnaud Lecollaire
 */
public class KinesisTestBinder extends
		AbstractTestBinder<KinesisMessageChannelBinder, ExtendedConsumerProperties<KinesisConsumerProperties>,
				ExtendedProducerProperties<KinesisProducerProperties>> {

	private final AmazonKinesisAsync amazonKinesis;

	private final GenericApplicationContext applicationContext;

	public KinesisTestBinder(AmazonKinesisAsync amazonKinesis, AmazonDynamoDBAsync dynamoDbClient,
		AmazonCloudWatch cloudWatchClient, KinesisBinderConfigurationProperties kinesisBinderConfigurationProperties) {

		this.applicationContext = new AnnotationConfigApplicationContext(Config.class);

		this.amazonKinesis = amazonKinesis;

		KinesisStreamProvisioner provisioningProvider = new KinesisStreamProvisioner(
				amazonKinesis, kinesisBinderConfigurationProperties);

		KinesisMessageChannelBinder binder = new TestKinesisMessageChannelBinder(
				amazonKinesis, dynamoDbClient, cloudWatchClient, kinesisBinderConfigurationProperties,
				provisioningProvider);

		binder.setApplicationContext(this.applicationContext);
		binder.setKinesisClientLibConfigurations(new ArrayList<>());

		setBinder(binder);
	}

	public GenericApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void cleanup() {
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		ListStreamsResult listStreamsResult = this.amazonKinesis
				.listStreams(listStreamsRequest);

		List<String> streamNames = listStreamsResult.getStreamNames();

		while (listStreamsResult.getHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(
						streamNames.get(streamNames.size() - 1));
			}
			listStreamsResult = this.amazonKinesis.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());
		}

		for (String stream : streamNames) {
			this.amazonKinesis.deleteStream(stream);
			while (true) {
				try {
					this.amazonKinesis.describeStream(stream);
					try {
						Thread.sleep(100);
					}
					catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException(ex);
					}
				}
				catch (ResourceNotFoundException ex) {
					break;
				}
			}
		}
	}

	/**
	 * Test configuration.
	 */
	@Configuration
	@EnableIntegration
	static class Config {

		@Bean
		public PartitionTestSupport partitionSupport() {
			return new PartitionTestSupport();
		}

	}

	private static class TestKinesisMessageChannelBinder
			extends KinesisMessageChannelBinder {

		TestKinesisMessageChannelBinder(AmazonKinesisAsync amazonKinesis,
				AmazonDynamoDBAsync dynamoDbClient,
				AmazonCloudWatch cloudWatchClient,
				KinesisBinderConfigurationProperties kinesisBinderConfigurationProperties,
				KinesisStreamProvisioner provisioningProvider) {

			super(kinesisBinderConfigurationProperties, provisioningProvider, amazonKinesis,
					new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")), dynamoDbClient, null, cloudWatchClient);
		}

		/*
		 * Some tests use multiple instance indexes for the same topic; we need to make
		 * the error infrastructure beans unique.
		 */
		@Override
		protected String errorsBaseName(ConsumerDestination destination, String group,
				ExtendedConsumerProperties<KinesisConsumerProperties> consumerProperties) {
			return super.errorsBaseName(destination, group, consumerProperties) + "-"
					+ consumerProperties.getInstanceIndex();
		}

		@Override
		protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
				String group,
				ExtendedConsumerProperties<KinesisConsumerProperties> properties) {

			MessageProducer messageProducer = super.createConsumerEndpoint(destination,
					group, properties);
			if (messageProducer instanceof KinesisMessageDrivenChannelAdapter) {
				DirectFieldAccessor dfa = new DirectFieldAccessor(messageProducer);
				dfa.setPropertyValue("describeStreamBackoff", 10);
				dfa.setPropertyValue("consumerBackoff", 10);
				dfa.setPropertyValue("idleBetweenPolls", 1);
			}
			return messageProducer;
		}

	}

}
