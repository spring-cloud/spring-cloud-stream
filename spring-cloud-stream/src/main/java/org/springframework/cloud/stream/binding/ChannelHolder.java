package org.springframework.cloud.stream.binding;

import org.springframework.messaging.MessageChannel;


/**
 * Holds information about the channels exposed by the interface proxy, as well as
 * their status.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ChannelHolder {

	private String name;

	private MessageChannel messageChannel;

	private boolean bindable;

	public ChannelHolder(String name, MessageChannel messageChannel, boolean bindable) {
		this.name = name;
		this.messageChannel = messageChannel;
		this.bindable = bindable;
	}

	public String getName() {
		return this.name;
	}

	public MessageChannel getMessageChannel() {
		return this.messageChannel;
	}

	public boolean isBindable() {
		return this.bindable;
	}
}
