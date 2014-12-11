package org.springframework.cloud.bus.amqp;

import org.junit.Test;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author Dave Syer
 */
public class AmqpBusAutoConfigurationTests {

	@Test
	public void contextStarts() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				AmqpBusAutoConfiguration.class, RabbitAutoConfiguration.class,
				PropertyPlaceholderAutoConfiguration.class, BusAutoConfiguration.class);
		context.close();
	}
}
