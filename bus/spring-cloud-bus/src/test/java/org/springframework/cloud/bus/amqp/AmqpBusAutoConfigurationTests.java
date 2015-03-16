package org.springframework.cloud.bus.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.cloud.bus.BusAutoConfiguration;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

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
	public void qualifiedConnectionFactory() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClasses(QualifiedConnectionFactory.class));
		assertTrue(context.containsBean("cloudBusExchange"));
		context.close();
	}

	@Test
	public void qualifiedConnectionFactoryAdmin() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClasses(QualifiedConnectionFactory.class));
		assertTrue(context.containsBean("amqpAdmin"));
		Object admin = context.getBean("amqpAdmin");
		Declarable declarable = context.getBean("cloudBusExchange", Declarable.class);
		assertEquals(1, declarable.getDeclaringAdmins().size());
		assertFalse(declarable.getDeclaringAdmins().contains(admin));
		context.close();
	}

	@Test
	public void unqualifiedConnectionFactory() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClasses(UnqualifiedConnectionFactory.class));
		assertTrue(context.containsBean("cloudBusExchange"));
		context.close();
	}

	@Test
	public void twoConnectionFactories() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClasses(TwoConnectionFactories.class));
		assertTrue(context.containsBean("cloudBusExchange"));
		context.close();
	}

	@Test
	public void notStartedIfBusDisabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED+":false");
		context.register(getConfigClasses());
		context.refresh();

		assertFalse(context.containsBean("cloudBusExchange"));

		context.close();
	}

	private Class<?>[] getConfigClasses(Class<?>... extras) {
		Class<?>[] defaults = new Class<?>[]{AmqpBusAutoConfiguration.class, RabbitAutoConfiguration.class,
				PropertyPlaceholderAutoConfiguration.class, BusAutoConfiguration.class};
		Class<?>[] result = new Class<?>[extras.length + defaults.length];
		System.arraycopy(extras, 0, result, 0, extras.length);
		System.arraycopy(defaults, 0, result, extras.length, defaults.length);
		return result;
	}
	
	@Configuration
	protected static class UnqualifiedConnectionFactory {
		@Bean
		public ConnectionFactory rabbitConnectionFactory(RabbitProperties config) {
			CachingConnectionFactory factory = new CachingConnectionFactory();
			return factory;
		}		
	}

	@Configuration
	protected static class QualifiedConnectionFactory {
		@Bean
		@BusConnectionFactory
		public ConnectionFactory rabbitConnectionFactory(RabbitProperties config) {
			CachingConnectionFactory factory = new CachingConnectionFactory();
			return factory;
		}		
	}
	
	@Configuration
	protected static class TwoConnectionFactories {
		@Bean
		@BusConnectionFactory
		public ConnectionFactory busConnectionFactory(RabbitProperties config) {
			CachingConnectionFactory factory = new CachingConnectionFactory();
			return factory;
		}		
		@Bean
		@Primary
		public ConnectionFactory rabbitConnectionFactory(RabbitProperties config) {
			CachingConnectionFactory factory = new CachingConnectionFactory();
			return factory;
		}		
	}
	
}
