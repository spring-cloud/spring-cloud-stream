/*
 * Copyright 2017-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.kafka.support.AbstractKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;

/**
 * Custom header mapper for Apache Kafka. This is identical to the {@link org.springframework.kafka.support.DefaultKafkaHeaderMapper}
 * from spring Kafka. This is provided for addressing some interoperability issues between Spring Cloud Stream 3.0.x
 * and 2.x apps, where mime types passed as regular {@link MimeType} in the header are not de-serialized properly.
 * It also suppresses certain internal headers that should never be propagated on output.
 *
 * Most headers in {@link org.springframework.kafka.support.KafkaHeaders} are not mapped onto outbound messages.
 * The exceptions are correlation and reply headers for request/reply
 * messaging.
 * Header types are added to a special header {@link #JSON_TYPES}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Soby Chacko
 * @author Byungjun You
 *
 * @since 3.0.0
 *
 */
public class BinderHeaderMapper extends AbstractKafkaHeaderMapper {

	private static final String NEGATE = "!";

	private static final String NEVER_ID = NEGATE + MessageHeaders.ID;

	private static final String NEVER_TIMESTAMP = NEGATE + MessageHeaders.TIMESTAMP;

	private static final String NEVER_DELIVERY_ATTEMPT = NEGATE + IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT;

	private static final String NEVER_NATIVE_HEADERS_PRESENT = NEGATE + BinderHeaders.NATIVE_HEADERS_PRESENT;

	private static final String JAVA_LANG_STRING = "java.lang.String";

	private static final List<String> DEFAULT_TRUSTED_PACKAGES =
			Arrays.asList(
					"java.lang",
					"java.net",
					"java.util",
					"org.springframework.util"
			);

	private static final List<String> DEFAULT_TO_STRING_CLASSES =
			Arrays.asList(
					"org.springframework.util.MimeType",
					"org.springframework.http.MediaType",
					"java.time.OffsetDateTime",
					"java.net.URI"
			);

	/**
	 * Header name for java types of other headers.
	 */
	public static final String JSON_TYPES = "spring_json_header_types";

	private final ObjectMapper objectMapper;

	private final Set<String> trustedPackages = new LinkedHashSet<>(DEFAULT_TRUSTED_PACKAGES);

	private final Set<String> toStringClasses = new LinkedHashSet<>(DEFAULT_TO_STRING_CLASSES);

	private boolean encodeStrings;

	/**
	 * Construct an instance with the default object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, most of the headers in
	 * {@link org.springframework.kafka.support.KafkaHeaders} are never mapped as headers since they represent data in
	 * consumer/producer records.
	 * @see #BinderHeaderMapper(ObjectMapper)
	 */
	public BinderHeaderMapper() {
		this(JacksonUtils.enhancedObjectMapper());
	}

	/**
	 * Construct an instance with the provided object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The patterns are applied in
	 * order, stopping on the first match (positive or negative). Patterns are negated by
	 * preceding them with "!". The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, most of the headers in
	 * {@link org.springframework.kafka.support.KafkaHeaders} are never mapped as headers since they represent data in
	 * consumer/producer records.
	 * @param objectMapper the object mapper.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public BinderHeaderMapper(ObjectMapper objectMapper) {
		this(objectMapper,
				NEVER_ID,
				NEVER_TIMESTAMP,
				NEVER_DELIVERY_ATTEMPT,
				NEVER_NATIVE_HEADERS_PRESENT,
				"*");
	}

	/**
	 * Construct an instance with a default object mapper and the provided header patterns
	 * for outbound headers; all inbound headers are mapped. The patterns are applied in
	 * order, stopping on the first match (positive or negative). Patterns are negated by
	 * preceding them with "!". The patterns will replace the default patterns; you
	 * generally should not map the {@code "id" and "timestamp"} headers. Note:
	 * most of the headers in {@link org.springframework.kafka.support.KafkaHeaders} are ever mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public BinderHeaderMapper(String... patterns) {
		this(new ObjectMapper(), patterns);
	}

	/**
	 * Construct an instance with the provided object mapper and the provided header
	 * patterns for outbound headers; all inbound headers are mapped. The patterns are
	 * applied in order, stopping on the first match (positive or negative). Patterns are
	 * negated by preceding them with "!". The patterns will replace the default patterns;
	 * you generally should not map the {@code "id" and "timestamp"} headers. Note: most
	 * of the headers in {@link org.springframework.kafka.support.KafkaHeaders} are never mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param objectMapper the object mapper.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public BinderHeaderMapper(ObjectMapper objectMapper, String... patterns) {
		super(patterns);
		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		Assert.noNullElements(patterns, "'patterns' must not have null elements");
		this.objectMapper = objectMapper;
		this.objectMapper
				.registerModule(new SimpleModule().addDeserializer(MimeType.class, new MimeTypeJsonDeserializer()));
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 */
	protected ObjectMapper getObjectMapper() {
		return this.objectMapper;
	}

	/**
	 * Provide direct access to the trusted packages set for subclasses.
	 * @return the trusted packages.
	 * @since 2.2
	 */
	protected Set<String> getTrustedPackages() {
		return this.trustedPackages;
	}

	/**
	 * Provide direct access to the toString() classes by subclasses.
	 * @return the toString() classes.
	 * @since 2.2
	 */
	protected Set<String> getToStringClasses() {
		return this.toStringClasses;
	}

	protected boolean isEncodeStrings() {
		return this.encodeStrings;
	}

	/**
	 * Set to true to encode String-valued headers as JSON ("..."), by default just the
	 * raw String value is converted to a byte array using the configured charset. Set to
	 * true if a consumer of the outbound record is using Spring for Apache Kafka version
	 * less than 2.3
	 * @param encodeStrings true to encode (default false).
	 * @since 2.3
	 */
	public void setEncodeStrings(boolean encodeStrings) {
		this.encodeStrings = encodeStrings;
	}

	/**
	 * Add packages to the trusted packages list (default {@code java.util, java.lang}) used
	 * when constructing objects from JSON.
	 * If any of the supplied packages is {@code "*"}, all packages are trusted.
	 * If a class for a non-trusted package is encountered, the header is returned to the
	 * application with value of type {@link NonTrustedHeaderType}.
	 * @param packagesToTrust the packages to trust.
	 */
	public void addTrustedPackages(String... packagesToTrust) {
		if (packagesToTrust != null) {
			for (String whiteList : packagesToTrust) {
				if ("*".equals(whiteList)) {
					this.trustedPackages.clear();
					break;
				}
				else {
					this.trustedPackages.add(whiteList);
				}
			}
		}
	}

	/**
	 * Add class names that the outbound mapper should perform toString() operations on
	 * before mapping.
	 * @param classNames the class names.
	 * @since 2.2
	 */
	public void addToStringClasses(String... classNames) {
		this.toStringClasses.addAll(Arrays.asList(classNames));
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Headers target) {
		final Map<String, String> jsonHeaders = new HashMap<>();
		final ObjectMapper headerObjectMapper = getObjectMapper();
		headers.forEach((key, rawValue) -> {
			if (matches(key, rawValue)) {
				Object valueToAdd = headerValueToAddOut(key, rawValue);
				if (valueToAdd instanceof byte[] valueToAddBytes) {
					target.add(new RecordHeader(key, valueToAddBytes));
				}
				else {
					try {
						String className = valueToAdd.getClass().getName();
						if (this.toStringClasses.contains(className)) {
							valueToAdd = valueToAdd.toString();
							className = JAVA_LANG_STRING;
						}
						if (!this.encodeStrings
								&& !MimeType.class.isAssignableFrom(rawValue.getClass())
								&& valueToAdd instanceof String stringValueToAdd) {
							target.add(new RecordHeader(key, stringValueToAdd.getBytes(getCharset())));
							className = JAVA_LANG_STRING;
						}
						else {
							target.add(new RecordHeader(key, headerObjectMapper.writeValueAsBytes(valueToAdd)));
						}
						jsonHeaders.put(key, className);
					}
					catch (Exception e) {
						logger.debug(e, () -> "Could not map " + key + " with type " + rawValue.getClass().getName());
					}
				}
			}
		});
		if (jsonHeaders.size() > 0) {
			try {
				target.add(new RecordHeader(JSON_TYPES, headerObjectMapper.writeValueAsBytes(jsonHeaders)));
			}
			catch (IllegalStateException | JsonProcessingException e) {
				logger.error(e, "Could not add json types header");
			}
		}
	}

	@Override
	public void toHeaders(Headers source, final Map<String, Object> headers) {
		final Map<String, String> jsonTypes = decodeJsonTypes(source);
		source.forEach(header -> {
			if (!(header.key().equals(JSON_TYPES))) {
				if (jsonTypes != null && jsonTypes.containsKey(header.key())) {
					String requestedType = jsonTypes.get(header.key());
					populateJsonValueHeader(header, requestedType, headers);
				}
				else {
					headers.put(header.key(), headerValueToAddIn(header));
				}
			}
		});
	}

	private void populateJsonValueHeader(Header header, String requestedType, Map<String, Object> headers) {
		Class<?> type = Object.class;
		boolean trusted = false;
		try {
			trusted = trusted(requestedType);
			if (trusted) {
				type = ClassUtils.forName(requestedType, null);
			}
		}
		catch (Exception e) {
			logger.error(e, () -> "Could not load class for header: " + header.key());
		}
		if (String.class.equals(type) && (header.value().length == 0 || header.value()[0] != '"')) {
			headers.put(header.key(), new String(header.value(), getCharset()));
		}
		else {
			if (trusted) {
				try {
					Object value = decodeValue(header, type);
					headers.put(header.key(), value);
				}
				catch (IOException e) {
					logger.error(e, () ->
							"Could not decode json type: " + new String(header.value()) + " for key: "
									+ header.key());
					headers.put(header.key(), header.value());
				}
			}
			else {
				headers.put(header.key(), new NonTrustedHeaderType(header.value(), requestedType));
			}
		}
	}

	private Object decodeValue(Header h, Class<?> type) throws IOException, LinkageError {
		ObjectMapper headerObjectMapper = getObjectMapper();
		Object value = headerObjectMapper.readValue(h.value(), type);
		if (type.equals(NonTrustedHeaderType.class)) {
			// Upstream NTHT propagated; may be trusted here...
			NonTrustedHeaderType nth = (NonTrustedHeaderType) value;
			if (trusted(nth.getUntrustedType())) {
				try {
					value = headerObjectMapper.readValue(nth.getHeaderValue(),
							ClassUtils.forName(nth.getUntrustedType(), null));
				}
				catch (Exception e) {
					logger.error(e, () -> "Could not decode header: " + nth);
				}
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private Map<String, String> decodeJsonTypes(Headers source) {
		Map<String, String> types = null;
		Header jsonTypes = source.lastHeader(JSON_TYPES);
		if (jsonTypes != null) {
			ObjectMapper headerObjectMapper = getObjectMapper();
			try {
				types = headerObjectMapper.readValue(jsonTypes.value(), Map.class);
			}
			catch (IOException e) {
				logger.error(e, () -> "Could not decode json types: " + new String(jsonTypes.value()));
			}
		}
		return types;
	}

	protected boolean trusted(String requestedType) {
		if (requestedType.equals(NonTrustedHeaderType.class.getName())) {
			return true;
		}
		if (!this.trustedPackages.isEmpty()) {
			int lastDot = requestedType.lastIndexOf('.');
			if (lastDot < 0) {
				return false;
			}
			String packageName = requestedType.substring(0, lastDot);
			for (String trustedPackage : this.trustedPackages) {
				if (packageName.equals(trustedPackage) || packageName.startsWith(trustedPackage + ".")) {
					return true;
				}
			}
			return false;
		}
		return true;
	}

	/**
	 * Add patterns for headers that should never be mapped.
	 * @param patterns the patterns.
	 * @return the modified patterns.
	 * @since 3.0.2
	 */
	public static String[] addNeverHeaderPatterns(List<String> patterns) {
		List<String> patternsToUse = new LinkedList<>(patterns);
		patternsToUse.add(0, NEVER_NATIVE_HEADERS_PRESENT);
		patternsToUse.add(0, NEVER_DELIVERY_ATTEMPT);
		patternsToUse.add(0, NEVER_TIMESTAMP);
		patternsToUse.add(0, NEVER_ID);
		return patternsToUse.toArray(new String[0]);
	}

	/**
	 * Remove never headers.
	 * @param headers the headers from which to remove the never headers.
	 * @since 3.0.2
	 */
	public static void removeNeverHeaders(Headers headers) {
		headers.remove(MessageHeaders.ID);
		headers.remove(MessageHeaders.TIMESTAMP);
		headers.remove(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
		headers.remove(BinderHeaders.NATIVE_HEADERS_PRESENT);
	}

	/**
	 * The {@link StdNodeBasedDeserializer} extension for {@link MimeType} deserialization.
	 * It is presented here for backward compatibility when older producers send {@link MimeType}
	 * headers as serialization version.
	 */
	private class MimeTypeJsonDeserializer extends StdNodeBasedDeserializer<MimeType> {

		private static final long serialVersionUID = 1L;

		MimeTypeJsonDeserializer() {
			super(MimeType.class);
		}

		@Override
		public MimeType convert(JsonNode root, DeserializationContext ctxt) throws IOException {
			if (root instanceof TextNode) {
				return MimeType.valueOf(root.asText());
			}
			else {
				JsonNode type = root.get("type");
				JsonNode subType = root.get("subtype");
				JsonNode parameters = root.get("parameters");
				Map<String, String> params =
						BinderHeaderMapper.this.objectMapper.readValue(parameters.traverse(),
								TypeFactory.defaultInstance()
										.constructMapType(HashMap.class, String.class, String.class));
				return new MimeType(type.asText(), subType.asText(), params);
			}
		}

	}

	/**
	 * Represents a header that could not be decoded due to an untrusted type.
	 */
	public static class NonTrustedHeaderType {

		private byte[] headerValue;

		private String untrustedType;

		public NonTrustedHeaderType() {
			super();
		}

		NonTrustedHeaderType(byte[] headerValue, String untrustedType) { // NOSONAR
			this.headerValue = headerValue; // NOSONAR
			this.untrustedType = untrustedType;
		}


		public void setHeaderValue(byte[] headerValue) { // NOSONAR
			this.headerValue = headerValue; // NOSONAR array reference
		}

		public byte[] getHeaderValue() {
			return this.headerValue; // NOSONAR
		}

		public void setUntrustedType(String untrustedType) {
			this.untrustedType = untrustedType;
		}

		public String getUntrustedType() {
			return this.untrustedType;
		}

		@Override
		public String toString() {
			try {
				return "NonTrustedHeaderType [headerValue=" + new String(this.headerValue, StandardCharsets.UTF_8)
						+ ", untrustedType=" + this.untrustedType + "]";
			}
			catch (@SuppressWarnings("unused") Exception e) {
				return "NonTrustedHeaderType [headerValue=" + Arrays.toString(this.headerValue) + ", untrustedType="
						+ this.untrustedType + "]";
			}
		}
	}
}
