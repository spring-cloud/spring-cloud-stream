package org.springframework.xd.dirt.integration.bus;

/**
 * Represents an out-of-band connection to the underlying middleware,
 * so that tests can check that some messages actually do (or do not)
 * transit through it.
 *
 * @author Eric Bottard
 */
public interface Spy {

	public Object receive(boolean expectNull) throws Exception;
}
