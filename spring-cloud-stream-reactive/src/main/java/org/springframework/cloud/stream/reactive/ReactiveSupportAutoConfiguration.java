/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marius Bogoevici
 */
@Configuration
@ConditionalOnBean(BindingService.class)
public class ReactiveSupportAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(MessageChannelToInputFluxParameterAdapter.class)
	public MessageChannelToInputFluxParameterAdapter messageChannelToInputFluxArgumentAdapter(
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		return new MessageChannelToInputFluxParameterAdapter(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered());
	}

	@Bean
	@ConditionalOnMissingBean(MessageChannelToFluxSenderParameterAdapter.class)
	public MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter() {
		return new MessageChannelToFluxSenderParameterAdapter();
	}

	@Bean
	@ConditionalOnMissingBean(PublisherToMessageChannelResultAdapter.class)
	public PublisherToMessageChannelResultAdapter fluxToMessageChannelResultAdapter() {
		return new PublisherToMessageChannelResultAdapter();
	}

	@Bean
	public static StreamEmitterAnnotationBeanPostProcessor streamEmitterAnnotationBeanPostProcessor() {
		return new StreamEmitterAnnotationBeanPostProcessor();
	}

	@Configuration
	@ConditionalOnClass(name = "rx.Observable")
	public static class RxJava1SupportConfiguration {

		@Bean
		@ConditionalOnMissingBean(MessageChannelToInputObservableParameterAdapter.class)
		public MessageChannelToInputObservableParameterAdapter messageChannelToInputObservableArgumentAdapter(
				MessageChannelToInputFluxParameterAdapter messageChannelToFluxArgumentAdapter) {
			return new MessageChannelToInputObservableParameterAdapter(messageChannelToFluxArgumentAdapter);
		}

		@Bean
		@ConditionalOnMissingBean(MessageChannelToObservableSenderParameterAdapter.class)
		public MessageChannelToObservableSenderParameterAdapter messageChannelToObservableSenderArgumentAdapter(
				MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter) {
			return new MessageChannelToObservableSenderParameterAdapter(messageChannelToFluxSenderArgumentAdapter);
		}

		@Bean
		@ConditionalOnMissingBean(ObservableToMessageChannelResultAdapter.class)
		public ObservableToMessageChannelResultAdapter observableToMessageChannelResultAdapter(
				PublisherToMessageChannelResultAdapter publisherToMessageChannelResultAdapter) {
			return new ObservableToMessageChannelResultAdapter(publisherToMessageChannelResultAdapter);
		}
	}
}
