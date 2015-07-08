/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.xd.dirt.plugins.stream;

import static org.springframework.xd.module.ModuleType.processor;
import static org.springframework.xd.module.ModuleType.sink;
import static org.springframework.xd.module.ModuleType.source;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.util.MimeType;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.options.FlattenedCompositeModuleOptionsMetadata;
import org.springframework.xd.module.options.ModuleOptionsMetadata;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;
import org.springframework.xd.module.options.PojoModuleOptionsMetadata;
import org.springframework.xd.module.options.spi.ModuleOption;

/**
 * A {@link ModuleOptionsMetadataResolver} that will dynamically add {@code inputType} and {@code outputType} options to
 * every module, according to their type.
 * 
 * @see ModuleTypeConversionPlugin
 * @author Eric Bottard
 * @author David Turanski
 */
public class ModuleTypeConversionPluginMetadataResolver implements ModuleOptionsMetadataResolver {

	private final GenericConversionService conversionService = new GenericConversionService();

	public ModuleTypeConversionPluginMetadataResolver() {
		conversionService.addConverter(new CustomMimeTypeConverter());
	}


	@Override
	public ModuleOptionsMetadata resolve(ModuleDefinition moduleDefinition) {
		List<ModuleOptionsMetadata> moms = new ArrayList<ModuleOptionsMetadata>();
		ModuleType type = moduleDefinition.getType();
		if (type == source || type == processor) {
			moms.add(new PojoModuleOptionsMetadata(OutputOptionsMetadata.class, conversionService));
		}
		if (type == sink || type == processor) {
			moms.add(new PojoModuleOptionsMetadata(InputOptionsMetadata.class, conversionService));
		}

		// Don't force deep layering if it's not needed
		switch (moms.size()) {
			case 0:
				return null;
			case 1:
				return moms.iterator().next();
			default:
				return new FlattenedCompositeModuleOptionsMetadata(moms);
		}
	}

	/**
	 * Provides info about the {@code inputType} option.
	 * 
	 * @author Eric Bottard
	 */
	@SuppressWarnings("unused")
	private static class InputOptionsMetadata {

		private MimeType inputType;

		public MimeType getInputType() {
			return inputType;
		}

		@ModuleOption("how this module should interpret messages it consumes")
		public void setInputType(MimeType inputType) {
			this.inputType = inputType;
		}
	}

	/**
	 * Provides info about the {@code outputType} option.
	 * 
	 * @author Eric Bottard
	 */
	@SuppressWarnings("unused")
	private static class OutputOptionsMetadata {

		private MimeType outputType;

		public MimeType getOutputType() {
			return outputType;
		}


		@ModuleOption("how this module should emit messages it produces")
		public void setOutputType(MimeType outputType) {
			this.outputType = outputType;
		}

	}

}
