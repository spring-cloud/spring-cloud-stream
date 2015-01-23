package org.springframework.cloud.bus.amqp;

import org.junit.Test;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.cloud.bus.BusAutoConfiguration.SPRING_CLOUD_BUS_ENABLED;

/**
 * @author Dave Syer
 */
public class AmqpBusAutoConfigurationTests {

	@Test
	public void contextStarts() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClasses());
		assertTrue(context.containsBean("cloudBusExchange"));
		context.close();
	}

	@Test
	public void notStartedIfBusDisabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, SPRING_CLOUD_BUS_ENABLED+":false");
		context.register(getConfigClasses());
		context.refresh();

		assertFalse(context.containsBean("cloudBusExchange"));

		context.close();
	}

	private Class[] getConfigClasses() {
		return new Class[]{AmqpBusAutoConfiguration.class, RabbitAutoConfiguration.class,
				PropertyPlaceholderAutoConfiguration.class, BusAutoConfiguration.class};
	}
}
