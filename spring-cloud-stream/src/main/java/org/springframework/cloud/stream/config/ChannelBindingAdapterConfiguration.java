/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.Properties;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binding.ChannelBindingAdapter;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.BinderAwareRouterBeanPostProcessor;
import org.springframework.cloud.stream.binding.ChannelBindingLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.messaging.core.DestinationResolver;

/**
 * Configuration class that provides necessary beans for {@link MessageChannel} binding.
 *
 * @author Dave Syer
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@EnableConfigurationProperties(ChannelBindingProperties.class)
public class ChannelBindingAdapterConfiguration {

	@Bean
	@ConditionalOnMissingBean(ChannelBindingAdapter.class)
	public ChannelBindingAdapter bindingAdapter(ChannelBindingProperties module,
			Binder<MessageChannel> binder, ConfigurableListableBeanFactory beanFactory) {
		return new ChannelBindingAdapter(module, binder);
	}

	@Bean
	@DependsOn("bindingAdapter")
	public ChannelBindingLifecycle channelBindingLifecycle() {
		return new ChannelBindingLifecycle();
	}

	@Bean
	public BinderAwareChannelResolver binderAwareChannelResolver(
			Binder<MessageChannel> binder) {
		return new BinderAwareChannelResolver(binder, new Properties());
	}

	// IMPORTANT: Nested class to avoid instantiating all of the above early
	@Configuration
	protected static class PostProcessorConfiguration {

		private BinderAwareChannelResolver binderAwareChannelResolver;

		@Bean
		public BinderAwareRouterBeanPostProcessor binderAwareRouterBeanPostProcessor(
				final ConfigurableListableBeanFactory beanFactory) {
			// IMPORTANT: Lazy delegate to avoid instantiating all of the above early
			return new BinderAwareRouterBeanPostProcessor(
					new DestinationResolver<MessageChannel>() {

						@Override
						public MessageChannel resolveDestination(String name)
								throws DestinationResolutionException {
							if (PostProcessorConfiguration.this.binderAwareChannelResolver == null) {
								PostProcessorConfiguration.this.binderAwareChannelResolver = BeanFactoryUtils
										.beanOfType(beanFactory,
												BinderAwareChannelResolver.class);
							}
							return PostProcessorConfiguration.this.binderAwareChannelResolver
									.resolveDestination(name);
						}

					});
		}

	}

}
