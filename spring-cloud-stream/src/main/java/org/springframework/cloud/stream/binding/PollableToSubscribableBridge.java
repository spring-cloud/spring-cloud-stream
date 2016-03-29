package org.springframework.cloud.stream.binding;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.DirectHandler;
import org.springframework.cloud.stream.config.DefaultPollerProperties;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * A class that supports bridging Pollable channel to Subscribable channel.
 * This class needs to be instantiated as a bean so that the configuration properties are available runtime.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
@EnableConfigurationProperties(DefaultPollerProperties.class)
public class PollableToSubscribableBridge implements BeanFactoryAware {

	@Autowired
	private DefaultPollerProperties defaultPollerProperties;

	private ConfigurableListableBeanFactory beanFactory;

	boolean isPollable(Class<?> channelType) {
		return PollableChannel.class.isAssignableFrom(channelType);
	}

	void bridge(String name, PollableChannel pollableChannel, SubscribableChannel subscribableChannel) {
		ConsumerEndpointFactoryBean consumerEndpointFactoryBean = new ConsumerEndpointFactoryBean();
		consumerEndpointFactoryBean.setInputChannel(pollableChannel);
		PollerMetadata pollerMetadata = this.defaultPollerProperties.getPollerMetadata();
		consumerEndpointFactoryBean.setPollerMetadata(pollerMetadata);
		consumerEndpointFactoryBean
				.setHandler(new DirectHandler(
						subscribableChannel));
		consumerEndpointFactoryBean.setBeanName("consumerEndpointFactoryBean_" + name);
		consumerEndpointFactoryBean.setBeanFactory(this.beanFactory);
		try {
			consumerEndpointFactoryBean.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		consumerEndpointFactoryBean.start();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}
}
