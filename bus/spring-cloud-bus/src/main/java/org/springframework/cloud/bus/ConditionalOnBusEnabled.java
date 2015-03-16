package org.springframework.cloud.bus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * @author Spencer Gibb
 */
@ConditionalOnProperty(value = ConditionalOnBusEnabled.SPRING_CLOUD_BUS_ENABLED, matchIfMissing = true)
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface ConditionalOnBusEnabled {

	public static String SPRING_CLOUD_BUS_ENABLED = "spring.cloud.bus.amqp.enabled";
}
