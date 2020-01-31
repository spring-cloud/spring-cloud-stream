package org.springframework.cloud.stream.binder.test;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;

public class FunctionBindingTestUtils {

	public static void bind(ConfigurableApplicationContext applicationContext, Object function) {
		try {
			String functionName = function instanceof Function ? "function" : (function instanceof Consumer ? "consumer" : "supplier");
			System.setProperty("spring.cloud.function.definition", functionName);
			applicationContext.getBeanFactory().registerSingleton(functionName, function);


			InitializingBean functionBindingRegistrar = applicationContext.getBean("functionBindingRegistrar", InitializingBean.class);
			functionBindingRegistrar.afterPropertiesSet();

			BindableProxyFactory bindingProxy = applicationContext.getBean("&" + functionName + "_binding", BindableProxyFactory.class);
			bindingProxy.afterPropertiesSet();

			InitializingBean functionBinder = applicationContext.getBean("functionInitializer", InitializingBean.class);
			functionBinder.afterPropertiesSet();

			BindingServiceProperties bindingProperties = applicationContext.getBean(BindingServiceProperties.class);
			String inputBindingName = functionName + "-in-0";
			String outputBindingName = functionName + "-out-0";
			Map<String, BindingProperties> bindings = bindingProperties.getBindings();
			BindingProperties inputProperties = bindings.get(inputBindingName);
			BindingProperties outputProperties = bindings.get(outputBindingName);
			ConsumerProperties consumerProperties = inputProperties.getConsumer();
			ProducerProperties producerProperties = outputProperties.getProducer();


			TestChannelBinder binder = applicationContext.getBean(TestChannelBinder.class);
			if (function instanceof Supplier || function instanceof Function) {
				Binding<MessageChannel> bindProducer = binder.bindProducer(outputProperties.getDestination(),
						applicationContext.getBean(outputBindingName, MessageChannel.class),
						producerProperties == null ? new ProducerProperties() : producerProperties);
				bindProducer.start();
			}
			if (function instanceof Consumer || function instanceof Function) {
				Binding<MessageChannel> bindConsumer = binder.bindConsumer(inputProperties.getDestination(), null,
						applicationContext.getBean(inputBindingName, MessageChannel.class),
						consumerProperties == null ? new ConsumerProperties() : consumerProperties);
				bindConsumer.start();
			}
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to bind function", e);
		}
		finally {
			System.clearProperty("spring.cloud.function.definition");
		}
	}
}
