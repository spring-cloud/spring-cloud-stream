package org.springframework.cloud.stream.binding;

import org.springframework.messaging.MessageChannel;


/**
 * Holds information about the channels exposed by the interface proxy, as well as
 * their status.
 *
 * @author Marius Bogoevici
 */
public class ChannelHolder {

	private MessageChannel messageChannel;

	private boolean bindable;

	public ChannelHolder(MessageChannel messageChannel, boolean bindable) {
		this.messageChannel = messageChannel;
		this.bindable = bindable;
	}

	public MessageChannel getMessageChannel() {
		return this.messageChannel;
	}

	public boolean isBindable() {
		return this.bindable;
	}
}
