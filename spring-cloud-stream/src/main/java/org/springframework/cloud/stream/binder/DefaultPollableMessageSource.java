/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.ArrayList;
import java.util.List;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.context.Lifecycle;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.endpoint.MessageSourcePollingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;

/**
 * The default implementation of a {@link PollableMessageSource}.
 *
 * @author Gary Russell
 * @since 5.0
 *
 */
public class DefaultPollableMessageSource implements PollableMessageSource, Lifecycle {

	private final List<ChannelInterceptor> interceptors = new ArrayList<>();

	private MessageSource<?> source;

	private MessageSourcePollingTemplate pollingTemplate;

	private volatile boolean running;

	public void setSource(MessageSource<?> source) {
		ProxyFactory pf = new ProxyFactory(source);
		class ReceiveAdvice implements MethodInterceptor {

			private final List<ChannelInterceptor> interceptors = new ArrayList<>();

			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				Object result = invocation.proceed();
				if (result instanceof Message) {
					Message<?> received = (Message<?>) result;
					for (ChannelInterceptor interceptor : this.interceptors) {
						received = interceptor.preSend(received, null);
						if (received == null) {
							break;
						}
					}
					return received;
				}
				return result;
			}

		}
		final ReceiveAdvice advice = new ReceiveAdvice();
		advice.interceptors.addAll(this.interceptors);
		NameMatchMethodPointcutAdvisor sourceAdvisor = new NameMatchMethodPointcutAdvisor(advice);
		sourceAdvisor.addMethodName("receive");
		pf.addAdvisor(sourceAdvisor);
		this.source = (MessageSource<?>) pf.getProxy();
		this.pollingTemplate = new MessageSourcePollingTemplate(this.source);
	}

	public void addInterceptor(ChannelInterceptor interceptor) {
		this.interceptors.add(interceptor);
	}

	public void addInterceptor(int index, ChannelInterceptor interceptor) {
		this.interceptors.add(index, interceptor);
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public void start() {
		if (!this.running && this.source instanceof Lifecycle) {
			((Lifecycle) this.source).start();
		}
		this.running = true;
	}

	@Override
	public void stop() {
		if (this.running && this.source instanceof Lifecycle) {
			((Lifecycle) this.source).stop();
		}
		this.running = false;
	}

	@Override
	public boolean poll(MessageHandler handler) {
		return this.pollingTemplate.poll(handler);
	}

}
