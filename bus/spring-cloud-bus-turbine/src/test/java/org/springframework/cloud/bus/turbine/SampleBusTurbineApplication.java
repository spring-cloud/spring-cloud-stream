package org.springframework.cloud.bus.turbine;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * @author Spencer Gibb
 */
@EnableAutoConfiguration
@EnableBusTurbine
public class SampleBusTurbineApplication {
	public static void main(String[] args) {
		new SpringApplicationBuilder()
				.sources(SampleBusTurbineApplication.class)
				.run(args);
	}
}
