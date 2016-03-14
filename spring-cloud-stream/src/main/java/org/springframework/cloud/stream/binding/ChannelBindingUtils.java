package org.springframework.cloud.stream.binding;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.DirectHandler;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.support.PeriodicTrigger;

/**
 * An utility class that provides helper methods for channel binding.
 * This class needs to be instantiated as a bean so that the configuration properties are available runtime.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 */
public class ChannelBindingUtils implements BeanFactoryAware {

	public static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	public static final String POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".pollableBridge.interval";

	@Value("${" + POLLABLE_BRIDGE_INTERVAL_PROPERTY_NAME + ":1000}")
	private int pollableBridgeDefaultFrequency;

	private ConfigurableListableBeanFactory beanFactory;

	boolean isPollable(Class<?> channelType) {
		return PollableChannel.class.isAssignableFrom(channelType);
	}

	void bridgeSubscribableToPollableChannel(SubscribableChannel sharedChannel, PollableChannel pollableChannel) {
		sharedChannel.subscribe(new DirectHandler(pollableChannel));
	}

	void bridgePollableToSubscribableChannel(String name, PollableChannel pollableChannel,
			SubscribableChannel subscribableChannel) {
		ConsumerEndpointFactoryBean consumerEndpointFactoryBean = new ConsumerEndpointFactoryBean();
		consumerEndpointFactoryBean.setInputChannel(pollableChannel);
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(this.pollableBridgeDefaultFrequency));
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
