package org.springframework.cloud.stream.internal;

/**
 * Contains the names of properties for the internal use of Spring Cloud Stream.
 * 
 * @author Marius Bogoevici
 */
public abstract class InternalPropertyNames {

	public static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	public static final String NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX + ".namespace";

	public static final String SELF_CONTAINED_APP_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX
			+ ".selfContained";
}
