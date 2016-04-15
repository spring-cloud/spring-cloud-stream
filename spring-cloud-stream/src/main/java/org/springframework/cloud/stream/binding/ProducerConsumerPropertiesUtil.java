/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cloud.stream.binding;

import org.springframework.beans.BeanUtils;
import org.springframework.boot.bind.RelaxedDataBinder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.validation.beanvalidation.CustomValidatorBean;

/**
 * Helper class for operations involving producer/consumer properties.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ProducerConsumerPropertiesUtil {

		private static CustomValidatorBean validator;

		static {
			validator = new CustomValidatorBean();
			validator.afterPropertiesSet();
		}

		static void validate(Object properties) {
			RelaxedDataBinder dataBinder = new RelaxedDataBinder(properties);
			dataBinder.setValidator(validator);
			dataBinder.validate();
			if (dataBinder.getBindingResult().hasErrors()) {
				throw new IllegalStateException(dataBinder.getBindingResult().toString());
			}
		}

		static ProducerProperties getExtendedProducerProperties(ExtendedPropertiesBinder extendedPropertiesBinder, String outputChannelName,
				ProducerProperties producerProperties) {
			Object extension = extendedPropertiesBinder.getExtendedProducerProperties(outputChannelName);
			ExtendedProducerProperties extendedProducerProperties = new ExtendedProducerProperties<>(extension);
			BeanUtils.copyProperties(producerProperties, extendedProducerProperties);
			return extendedProducerProperties;
		}

		static ConsumerProperties getExtendedConsumerProperties(ExtendedPropertiesBinder extendedPropertiesBinder, String inputChannelName,
				ConsumerProperties consumerProperties) {
			Object extension = extendedPropertiesBinder.getExtendedConsumerProperties(inputChannelName);
			ExtendedConsumerProperties extendedConsumerProperties = new ExtendedConsumerProperties(extension);
			BeanUtils.copyProperties(consumerProperties, extendedConsumerProperties);
			return extendedConsumerProperties;
		}
}
