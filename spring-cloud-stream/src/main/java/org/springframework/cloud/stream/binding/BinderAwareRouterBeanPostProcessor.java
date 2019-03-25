/*
 * Copyright 2013-2017 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.integration.router.AbstractMappingMessageRouter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolver;

/**
 * A {@link BeanPostProcessor} that sets a {@link BinderAwareChannelResolver} on any bean
 * of type {@link AbstractMappingMessageRouter} within the context.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Oleg Zhurakousky
 *
 * @deprecated as of 2.0, will be renamed/replaced as it is no longer a BPP and naming is a bit confusing
 */
@Deprecated
public class BinderAwareRouterBeanPostProcessor {

	public BinderAwareRouterBeanPostProcessor(AbstractMappingMessageRouter[] routers, DestinationResolver<MessageChannel> channelResolver) {
		if (routers != null) {
			for (AbstractMappingMessageRouter router : routers) {
				router.setChannelResolver(channelResolver);
			}
		}
	}

}
