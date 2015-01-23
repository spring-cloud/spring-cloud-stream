package org.springframework.cloud.bus;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import static org.springframework.cloud.bus.BusAutoConfiguration.SPRING_CLOUD_BUS_ENABLED;

/**
 * Match if spring.cloud.bus.enabled is missing or not false
 * @author Spencer Gibb
 */
class OnBusEnabled extends SpringBootCondition {
	@Override
	public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
		RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(context.getEnvironment());
		String enabled = resolver.getProperty(SPRING_CLOUD_BUS_ENABLED);

		if (!"false".equalsIgnoreCase(enabled)) {
			return ConditionOutcome.match();
		}
		return ConditionOutcome.noMatch(SPRING_CLOUD_BUS_ENABLED + " is " + enabled);
	}
}
