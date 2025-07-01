/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.annotation.StreamRetryTemplate;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.expression.EvaluationContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for {@link Binder} implementations.
 *
 * @param <T> outbound bind target class
 * @param <C> consumer properties class
 * @param <P> producer properties class
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 * @author Nicolas Homble
 */
public abstract class AbstractBinder<T, C extends ConsumerProperties, P extends ProducerProperties>
		implements ApplicationContextAware, InitializingBean, Binder<T, C, P> {

	/**
	 * The delimiter between a group and index when constructing a binder
	 * consumer/producer.
	 */
	private static final String GROUP_INDEX_DELIMITER = ".";

	protected final Log logger = LogFactory.getLog(getClass());

	private volatile GenericApplicationContext applicationContext;

	private volatile EvaluationContext evaluationContext;

	@Autowired(required = false)
	@StreamRetryTemplate
	private Map<String, RetryTemplate> consumerBindingRetryTemplates;

	/**
	 * For binder implementations that support a prefix, apply the prefix to the name.
	 * @param prefix the prefix.
	 * @param name the name.
	 * @return name with the prefix
	 */
	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	/**
	 * For binder implementations that support dead lettering, construct the name of the
	 * dead letter entity for the underlying pipe name.
	 * @param name the name.
	 * @return name with DLQ suffix
	 */
	public static String constructDLQName(String name) {
		return name + ".dlq";
	}

	protected AbstractApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		Assert.isInstanceOf(GenericApplicationContext.class, applicationContext);
		this.applicationContext = (GenericApplicationContext) applicationContext;
		Map<String, EvaluationContext> beansOfType = this.applicationContext.getBeansOfType(EvaluationContext.class);
		if (beansOfType.size() > 0) {
			this.evaluationContext = beansOfType.values().iterator().next();
		}

	}

	protected ConfigurableListableBeanFactory getBeanFactory() {
		return this.applicationContext.getBeanFactory();
	}

	protected EvaluationContext getEvaluationContext() {
		return this.evaluationContext;
	}

	@Override
	public final void afterPropertiesSet() throws Exception {
		Assert.notNull(this.applicationContext,
				"The 'applicationContext' property must not be null");
		if (this.evaluationContext == null) {
//			this.evaluationContext = ExpressionUtils
//					.createStandardEvaluationContext(getBeanFactory());
		}
		onInit();
	}

	/**
	 * Subclasses may implement this method to perform any necessary initialization. It
	 * will be invoked from {@link #afterPropertiesSet()} which is itself {@code final}.
	 * @throws Exception when init fails
	 */
	protected void onInit() throws Exception {
		// no-op default
	}

	@Override
	public final Binding<T> bindConsumer(String name, String group, T target,
			C properties) {
		if (!StringUtils.hasText(group)) {
			Assert.isTrue(!properties.isPartitioned(),
					"A consumer group is required for a partitioned subscription");
		}
		return doBindConsumer(name, group, target, properties);
	}

	protected abstract Binding<T> doBindConsumer(String name, String group, T inputTarget,
			C properties);

	@Override
	public final Binding<T> bindProducer(String name, T outboundBindTarget,
			P properties) {
		return doBindProducer(name, outboundBindTarget, properties);
	}

	protected abstract Binding<T> doBindProducer(String name, T outboundBindTarget,
			P properties);

	/**
	 * Construct a name comprised of the name and group.
	 * @param name the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	protected final String groupedName(String name, String group) {
		return name + GROUP_INDEX_DELIMITER
				+ (StringUtils.hasText(group) ? group : "default");
	}

	/**
	 * Attempts to get {@link BindingServiceProperties} from application context.
	 *
	 * @return instance of {@link BindingServiceProperties} or null.
	 */
	protected BindingServiceProperties getBindingServiceProperties() {
		try {
			return getApplicationContext().getBean(BindingServiceProperties.class);
		}
		catch (Exception e) {
			// ignore
			return null;
		}
	}

	/**
	 * Create and configure a default retry template unless one has already been provided
	 * via @Bean by an application.
	 * @param properties The properties.
	 * @return The retry template
	 */
	protected RetryTemplate buildRetryTemplate(ConsumerProperties properties) {
		RetryTemplate rt;
		if (CollectionUtils.isEmpty(this.consumerBindingRetryTemplates)) {
			rt = new RetryTemplate();
			SimpleRetryPolicy retryPolicy = CollectionUtils
					.isEmpty(properties.getRetryableExceptions())
							? new SimpleRetryPolicy(properties.getMaxAttempts())
							: new SimpleRetryPolicy(properties.getMaxAttempts(),
									properties.getRetryableExceptions(), true,
									properties.isDefaultRetryable());

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval());
			backOffPolicy.setMultiplier(properties.getBackOffMultiplier());
			backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval());
			rt.setRetryPolicy(retryPolicy);
			rt.setBackOffPolicy(backOffPolicy);
		}
		else {
			rt = StringUtils.hasText(properties.getRetryTemplateName())
					? this.consumerBindingRetryTemplates
							.get(properties.getRetryTemplateName())
					: this.consumerBindingRetryTemplates.values().iterator().next();
		}
		return rt;
	}

}
