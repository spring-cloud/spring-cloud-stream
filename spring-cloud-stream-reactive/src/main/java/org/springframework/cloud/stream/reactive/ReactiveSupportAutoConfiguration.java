/*
 * Copyright 2016 the original author or authors.
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

import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.scheduler.Schedulers;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.reactive.reactor.core.scheduler.NoInterruptOnCancelSchedulerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ClassUtils;

/**
 * @author Marius Bogoevici
 */
@Configuration
@ConditionalOnBean(BindingService.class)
public class ReactiveSupportAutoConfiguration {

	private static Log log = LogFactory.getLog(ReactiveSupportAutoConfiguration.class);

	static {
		try {
			// Override Schedulers.Factory for Reactor 3.0.0 to work around
			// https://github.com/reactor/reactor-core/issues/159
			// To be removed once Reactor 3.0.1+ is used
			Class<?> executorServiceSchedulerClass = ClassUtils.forName("reactor.core.scheduler.ExecutorServiceScheduler", null);
			try {
				// simple check that the construction version with the cancellation option is not on the classpath
				executorServiceSchedulerClass.getConstructor(ExecutorService.class, Boolean.class);
			}
			catch (NoSuchMethodException e) {
				if (log.isDebugEnabled()) {
					log.debug("Overriding Schedulers for Reactor");
				}
				Schedulers.setFactory(new NoInterruptOnCancelSchedulerFactory());
			}
		}
		catch (ClassNotFoundException e) {
			// Ignore if absent - means that we're on a different Reactor version than expected
			if (log.isInfoEnabled()) {
				log.info("Class reactor.core.scheduler.ExecutorServiceScheduler not found. Check Reactor version.");
			}
		}
	}

	@Bean
	public MessageChannelToInputFluxParameterAdapter messageChannelToInputFluxArgumentAdapter(
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		return new MessageChannelToInputFluxParameterAdapter(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered());
	}

	@Bean
	public MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter() {
		return new MessageChannelToFluxSenderParameterAdapter();
	}

	@Bean
	public FluxToMessageChannelResultAdapter fluxToMessageChannelResultAdapter() {
		return new FluxToMessageChannelResultAdapter();
	}

	@Configuration
	@ConditionalOnClass(name = "rx.Observable")
	public static class RxJava1SupportConfiguration {

		@Bean
		public MessageChannelToInputObservableParameterAdapter messageChannelToInputObservableArgumentAdapter(
				MessageChannelToInputFluxParameterAdapter messageChannelToFluxArgumentAdapter) {
			return new MessageChannelToInputObservableParameterAdapter(messageChannelToFluxArgumentAdapter);
		}

		@Bean
		public MessageChannelToObservableSenderParameterAdapter messageChannelToObservableSenderArgumentAdapter(
				MessageChannelToFluxSenderParameterAdapter messageChannelToFluxSenderArgumentAdapter) {
			return new MessageChannelToObservableSenderParameterAdapter(messageChannelToFluxSenderArgumentAdapter);
		}

		@Bean
		public ObservableToMessageChannelResultAdapter
		observableToMessageChannelResultAdapter(
				FluxToMessageChannelResultAdapter fluxToMessageChannelResultAdapter) {
			return new ObservableToMessageChannelResultAdapter(fluxToMessageChannelResultAdapter);
		}
	}
}
