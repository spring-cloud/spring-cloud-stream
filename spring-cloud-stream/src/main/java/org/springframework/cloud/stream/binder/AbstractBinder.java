/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON;
import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.expression.EvaluationContext;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
public abstract class AbstractBinder<T, C extends ConsumerProperties, P extends ProducerProperties> implements ApplicationContextAware, InitializingBean, Binder<T, C, P> {

	protected static final String PARTITION_HEADER = "partition";

	/**
	 * The delimiter between a group and index when constructing a binder consumer/producer.
	 */
	private static final String GROUP_INDEX_DELIMITER = ".";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private volatile AbstractApplicationContext applicationContext;

	private volatile Codec codec;

	private final StringConvertingContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	protected volatile EvaluationContext evaluationContext;

	protected volatile PartitionSelectorStrategy partitionSelector;

	// Payload type cache
	private volatile Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	/**
	 * For binder implementations that support a prefix, apply the prefix to the name.
	 * @param prefix the prefix.
	 * @param name   the name.
	 */
	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	/**
	 * For binder implementations that support dead lettering, construct the name of the dead letter entity for the
	 * underlying pipe name.
	 * @param name the name.
	 */
	public static String constructDLQName(String name) {
		return name + ".dlq";
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(AbstractApplicationContext.class, applicationContext);
		this.applicationContext = (AbstractApplicationContext) applicationContext;
	}

	protected AbstractApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	protected ConfigurableListableBeanFactory getBeanFactory() {
		return this.applicationContext.getBeanFactory();
	}

	public void setCodec(Codec codec) {
		this.codec = codec;
	}

	public void setIntegrationEvaluationContext(EvaluationContext evaluationContext) {
		this.evaluationContext = evaluationContext;
	}

	@Override
	public final void afterPropertiesSet() throws Exception {
		Assert.notNull(this.applicationContext, "The 'applicationContext' property must not be null");
		if (this.evaluationContext == null) {
			this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
		}
		onInit();
	}

	/**
	 * Subclasses may implement this method to perform any necessary initialization.
	 * It will be invoked from {@link #afterPropertiesSet()} which is itself {@code final}.
	 */
	protected void onInit() throws Exception {
		// no-op default
	}

	@Override
	public final Binding<T> bindConsumer(String name, String group, T target, C properties) {
		if (StringUtils.isEmpty(group)) {
			Assert.isTrue(!properties.isPartitioned(),
					"A consumer group is required for a partitioned subscription");
		}
		return doBindConsumer(name, group, target, properties);
	}

	protected abstract Binding<T> doBindConsumer(String name, String group, T inputTarget, C properties);

	@Override
	public final Binding<T> bindProducer(String name, T outboundBindTarget, P properties) {
		return doBindProducer(name, outboundBindTarget, properties);
	}

	protected abstract Binding<T> doBindProducer(String name, T outboundBindTarget, P properties);

	/**
	 * Construct a name comprised of the name and group.
	 * @param name  the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	protected final String groupedName(String name, String group) {
		if (!StringUtils.hasText(group)) {
			group = "default";
		}
		return name + GROUP_INDEX_DELIMITER + group;
	}

	protected final MessageValues serializePayloadIfNecessary(Message<?> message) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);

		//Pass content type as String since some transport adapters will exclude CONTENT_TYPE Header otherwise
		Object contentType = JavaClassMimeTypeConversion.mimeTypeFromObject(originalPayload).toString();
		Object payload = serializePayloadIfNecessary(originalPayload);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		return messageValues;
	}

	private byte[] serializePayloadIfNecessary(Object originalPayload) {
		if (originalPayload instanceof byte[]) {
			return (byte[]) originalPayload;
		}
		else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				if (originalPayload instanceof String) {
					return ((String) originalPayload).getBytes("UTF-8");
				}
				this.codec.encode(originalPayload, bos);
				return bos.toByteArray();
			}
			catch (IOException e) {
				throw new SerializationFailedException("unable to serialize payload ["
						+ originalPayload.getClass().getName() + "]", e);
			}
		}
	}

	protected final MessageValues deserializePayloadIfNecessary(Message<?> message) {
		return deserializePayloadIfNecessary(new MessageValues(message));
	}

	protected final MessageValues deserializePayloadIfNecessary(MessageValues messageValues) {
		Object originalPayload = messageValues.getPayload();
		MimeType contentType = this.contentTypeResolver.resolve(messageValues);
		Object payload = deserializePayload(originalPayload, contentType);
		if (payload != null) {
			messageValues.setPayload(payload);
			Object originalContentType = messageValues.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			// Reset content-type only if the original content type is not null (when receiving messages from
			// non-SCSt applications).
			if (originalContentType != null) {
				messageValues.put(MessageHeaders.CONTENT_TYPE, originalContentType);
				messageValues.remove(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			}
		}
		return messageValues;
	}

	private Object deserializePayload(Object payload, MimeType contentType) {
		if (payload instanceof byte[]) {
			if (contentType == null || APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			}
			else {
				return deserializePayload((byte[]) payload, contentType);
			}
		}
		return payload;
	}

	private Object deserializePayload(byte[] bytes, MimeType contentType) {
		if ("text".equalsIgnoreCase(contentType.getType()) || APPLICATION_JSON.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				throw new SerializationFailedException("unable to deserialize [java.lang.String]. Encoding not supported.",
						e);
			}
		}
		else {
			String className = JavaClassMimeTypeConversion.classNameFromMimeType(contentType);
			try {
				// Cache types to avoid unnecessary ClassUtils.forName calls.
				Class<?> targetType = this.payloadTypeCache.get(className);
				if (targetType == null) {
					targetType = ClassUtils.forName(className, null);
					this.payloadTypeCache.put(className, targetType);
				}
				return this.codec.decode(bytes, targetType);
			}
			catch (ClassNotFoundException e) {
				throw new SerializationFailedException("unable to deserialize [" + className + "]. Class not found.",
						e);//NOSONAR
			}
			catch (IOException e) {
				throw new SerializationFailedException("unable to deserialize [" + className + "]", e);
			}
		}
	}

	protected String buildPartitionRoutingExpression(String expressionRoot) {
		return "'" + expressionRoot + "-' + headers['" + PARTITION_HEADER + "']";
	}

	/**
	 * Create and configure a retry template if the consumer 'maxAttempts' property is set.
	 * @param properties The properties.
	 * @return The retry template, or null if retry is not enabled.
	 */
	protected RetryTemplate buildRetryTemplateIfRetryEnabled(ConsumerProperties properties) {
		int maxAttempts = properties.getMaxAttempts();
		if (maxAttempts > 1) {
			RetryTemplate template = new RetryTemplate();
			SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
			retryPolicy.setMaxAttempts(maxAttempts);
			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval());
			backOffPolicy.setMultiplier(properties.getBackOffMultiplier());
			backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval());
			template.setRetryPolicy(retryPolicy);
			template.setBackOffPolicy(backOffPolicy);
			return template;
		}
		else {
			return null;
		}
	}


	/**
	 * Handles representing any java class as a {@link MimeType}.
	 *
	 * @author David Turanski
	 * @author Ilayaperumal Gopinathan
	 */
	abstract static class JavaClassMimeTypeConversion {

		private static ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

		static MimeType mimeTypeFromObject(Object payload) {
			Assert.notNull(payload, "payload object cannot be null.");
			if (payload instanceof byte[]) {
				return MimeTypeUtils.APPLICATION_OCTET_STREAM;
			}
			if (payload instanceof String) {
				return MimeTypeUtils.TEXT_PLAIN;
			}
			String className = payload.getClass().getName();
			MimeType mimeType = mimeTypesCache.get(className);
			if (mimeType == null) {
				String modifiedClassName = className;
				if (payload.getClass().isArray()) {
					// Need to remove trailing ';' for an object array, e.g. "[Ljava.lang.String;" or multi-dimensional
					// "[[[Ljava.lang.String;"
					if (modifiedClassName.endsWith(";")) {
						modifiedClassName = modifiedClassName.substring(0, modifiedClassName.length() - 1);
					}
					// Wrap in quotes to handle the illegal '[' character
					modifiedClassName = "\"" + modifiedClassName + "\"";
				}
				mimeType = MimeType.valueOf("application/x-java-object;type=" + modifiedClassName);
				mimeTypesCache.put(className, mimeType);
			}
			return mimeType;
		}

		static String classNameFromMimeType(MimeType mimeType) {
			Assert.notNull(mimeType, "mimeType cannot be null.");
			String className = mimeType.getParameter("type");
			if (className == null) {
				return null;
			}
			//unwrap quotes if any
			className = className.replace("\"", "");

			// restore trailing ';'
			if (className.contains("[L")) {
				className += ";";
			}
			return className;
		}

	}

	/**
	 * Perform manual acknowledgement based on the metadata stored in the binder.
	 */
	public void doManualAck(LinkedList<MessageHeaders> messageHeaders) {
	}

}
