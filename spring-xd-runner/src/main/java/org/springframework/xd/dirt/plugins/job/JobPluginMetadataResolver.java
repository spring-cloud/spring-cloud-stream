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

package org.springframework.xd.dirt.plugins.job;

import static org.springframework.xd.module.ModuleType.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.validation.constraints.AssertTrue;

import org.springframework.xd.dirt.plugins.job.support.listener.XDJobListenerConstants;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.options.ModuleOptionsMetadata;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;
import org.springframework.xd.module.options.PojoModuleOptionsMetadata;
import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.spi.ProfileNamesProvider;
import org.springframework.xd.rest.domain.util.TimeUtils;


/**
 * A {@link ModuleOptionsMetadataResolver} that will dynamically add the module options (such as {@code makeUnique})
 * supported by job modules.
 *
 * @see JobPlugin
 * @author Eric Bottard
 * @author Ilayaperumal Gopinathan
 */
public class JobPluginMetadataResolver implements ModuleOptionsMetadataResolver {


	@Override
	public ModuleOptionsMetadata resolve(ModuleDefinition moduleDefinition) {
		if (moduleDefinition.getType() == job) {
			return new PojoModuleOptionsMetadata(JobOptionsMetadata.class);
		}
		else {
			return null;
		}
	}

	public static class JobOptionsMetadata implements ProfileNamesProvider {

		private boolean makeUnique = true;

		private String numberFormat = "";

		private String dateFormat = TimeUtils.DEFAULT_XD_DATE_FORMAT_PATTERN;

		private String listeners = "";

		private String DISABLE_OPTION = "disable";

		private final List<String> VALID_LISTENER_SUFFIXES = new ArrayList<String>(
				Arrays.asList(XDJobListenerConstants.XD_JOB_LISTENERS_SUFFIXES));

		private Collection<String> listenerProfilesToAdd = new ArrayList<String>();

		private static final Map<String, String> XD_JOB_LISTENER_PROFILES = new HashMap<String, String>();

		static {
			// Profile names and bean names are equal for simplicity
			XD_JOB_LISTENER_PROFILES.put(XDJobListenerConstants.JOB_EXECUTION_EVENTS_SUFFIX,
					XDJobListenerConstants.XD_JOB_EXECUTION_LISTENER_BEAN);
			XD_JOB_LISTENER_PROFILES.put(XDJobListenerConstants.STEP_EXECUTION_EVENTS_SUFFIX,
					XDJobListenerConstants.XD_STEP_EXECUTION_LISTENER_BEAN);
			XD_JOB_LISTENER_PROFILES.put(XDJobListenerConstants.CHUNK_EVENTS_SUFFIX,
					XDJobListenerConstants.XD_CHUNK_LISTENER_BEAN);
			XD_JOB_LISTENER_PROFILES.put(XDJobListenerConstants.ITEM_EVENTS_SUFFIX,
					XDJobListenerConstants.XD_ITEM_LISTENER_BEAN);
			XD_JOB_LISTENER_PROFILES.put(XDJobListenerConstants.SKIP_EVENTS_SUFFIX,
					XDJobListenerConstants.XD_SKIP_LISTENER_BEAN);
		}


		public boolean isMakeUnique() {
			return makeUnique;
		}

		public String getNumberFormat() {
			return numberFormat;
		}

		public String getDateFormat() {
			return dateFormat;
		}

		public String getListeners() {
			return listeners;
		}

		@ModuleOption("whether always allow re-invocation of this job")
		public void setMakeUnique(boolean makeUnique) {
			this.makeUnique = makeUnique;
		}

		@ModuleOption("the number format to use when parsing numeric parameters")
		public void setNumberFormat(String numberFormat) {
			this.numberFormat = numberFormat;
		}

		@ModuleOption("the date format to use when parsing date parameters")
		public void setDateFormat(String dateFormat) {
			this.dateFormat = dateFormat;
		}

		@AssertTrue(message = "must be 'disable' or a combination of [job,step,chunk,item and skip]")
		public boolean isListenersListValid() {
			List<String> validOptions = new ArrayList<String>(VALID_LISTENER_SUFFIXES);
			validOptions.add(DISABLE_OPTION);
			StringTokenizer tokenizer = new StringTokenizer(listeners, ",");
			while (tokenizer.hasMoreElements()) {
				String optionName = (String) tokenizer.nextElement();
				if (!validOptions.contains(optionName)) {
					return false;
				}
			}
			return true;
		}

		@ModuleOption("listeners from [job,step,chunk,item,skip] as csv or 'disable' (default enables all)")
		public void setListeners(String listeners) {
			this.listeners = listeners;
		}

		@Override
		public String[] profilesToActivate() {
			if (listeners.contains(DISABLE_OPTION)) {
				return NO_PROFILES;
			}
			else {
				StringTokenizer tokenizer = new StringTokenizer(listeners, ",");
				while (tokenizer.hasMoreElements()) {
					String suffixName = (String) tokenizer.nextElement();
					listenerProfilesToAdd.add(XD_JOB_LISTENER_PROFILES.get(suffixName));
				}
				if (listenerProfilesToAdd.isEmpty()) {
					listenerProfilesToAdd.addAll(XD_JOB_LISTENER_PROFILES.values());
				}
				return listenerProfilesToAdd.toArray(new String[listenerProfilesToAdd.size()]);
			}

		}
	}
}
