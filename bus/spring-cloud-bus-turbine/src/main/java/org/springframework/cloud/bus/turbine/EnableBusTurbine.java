package org.springframework.cloud.bus.turbine;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Run the RxNetty based Spring Cloud Bus Turbine server.
 * Based on Netflix Turbine 2
 *
 * @author Spencer Gibb
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(BusTurbineConfiguration.class)
public @interface EnableBusTurbine {
}
