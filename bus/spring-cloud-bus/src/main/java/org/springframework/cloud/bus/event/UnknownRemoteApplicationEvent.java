package org.springframework.cloud.bus.event;

/**
 *
 * @author Stefan Pfeiffer
 */
public class UnknownRemoteApplicationEvent extends RemoteApplicationEvent {

	protected String typeInfo;
	protected byte[] payload;

	@SuppressWarnings("unused")
	private UnknownRemoteApplicationEvent() {
		super();
		this.typeInfo = null;
		this.payload = null;
	}

	public UnknownRemoteApplicationEvent(Object source, String typeInfo, byte[] payload) {
		super(source, null, null);
		this.typeInfo = typeInfo;
		this.payload = payload;
	}

	public String getTypeInfo() {
		return this.typeInfo;
	}

	public byte[] getPayload() {
		return this.payload;
	}

	public String getPayloadAsString() {
		return new String(this.payload);
	}
}
