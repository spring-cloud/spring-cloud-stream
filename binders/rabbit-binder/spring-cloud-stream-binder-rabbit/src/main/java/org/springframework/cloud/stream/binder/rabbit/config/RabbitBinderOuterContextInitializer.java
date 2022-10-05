package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.boot.autoconfigure.amqp.ConnectionFactoryCustomizer;
import org.springframework.cloud.stream.binder.BinderOuterContextInitializer;
import org.springframework.context.support.GenericApplicationContext;

public class RabbitBinderOuterContextInitializer extends BinderOuterContextInitializer {

	@Override
	public void initialize(GenericApplicationContext applicationContext) {
		super.initialize(applicationContext);
		getOuterContext().getBeanProvider(ConnectionFactoryCustomizer.class).forEach((customizer) ->
				applicationContext.registerBean(ConnectionFactoryCustomizer.class, () -> customizer));
	}
}
