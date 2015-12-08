/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.annotationprocessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import org.springframework.boot.configurationprocessor.metadata.ConfigurationMetadata;
import org.springframework.boot.configurationprocessor.metadata.ItemMetadata;
import org.springframework.boot.configurationprocessor.metadata.JsonMarshaller;
import org.springframework.util.Assert;

/**
 * An annotation processor that generates Spring Boot compatible json metadata for
 * synthetic properties declared with {@link ImplicitExpression}.
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
@SupportedAnnotationTypes({ImplicitExpressionProcessor.IMPLICIT_EXPRESSION})
public class ImplicitExpressionProcessor extends AbstractProcessor {

	static final String IMPLICIT_EXPRESSION = "org.springframework.cloud.stream.annotationprocessor.ImplicitExpression";

	private static final String CONFIGURATION_PROPERTIES = "org.springframework.boot.context.properties.ConfigurationProperties";

	public static final String METADATA_FILE = "META-INF/implicit-expressions-spring-configuration-metadata.json";

	/**
	 * The suffix to chop off annotated field names.
	 */
	private static final String EXPRESSION_SUFFIX = "Expression";

	private Filer filer;

	private Elements elementUtils;

	private Set<Element> implicitExpressions = new HashSet<>();

	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latest();
	}

	@Override
	public synchronized void init(ProcessingEnvironment processingEnv) {
		super.init(processingEnv);
		filer = processingEnv.getFiler();
		elementUtils = processingEnv.getElementUtils();
	}

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		ConfigurationMetadata metadata = new ConfigurationMetadata();
		if (!roundEnv.processingOver()) {
			implicitExpressions.addAll(roundEnv.getElementsAnnotatedWith(elementUtils.getTypeElement(IMPLICIT_EXPRESSION)));
		}
		else {
			for (Element implicitExpression : implicitExpressions) {
				AnnotationMirror implicitExpressionAnnotation = getAnnotation(ImplicitExpression.class.getName(), implicitExpression.getAnnotationMirrors());
				AnnotationMirror configPropsAnnotation = getAnnotation(CONFIGURATION_PROPERTIES, implicitExpression.getEnclosingElement().getAnnotationMirrors());
				String prefix = attributeValue("prefix", configPropsAnnotation, "");
				TypeMirror stringDotClass = elementUtils.getTypeElement(String.class.getName()).asType();
				ItemMetadata item = ItemMetadata.newProperty(
						prefix,
						syntheticName(implicitExpression.getSimpleName().toString()),
						attributeValue("type", implicitExpressionAnnotation, stringDotClass).toString(),
						implicitExpression.getEnclosingElement().toString(),
						null,
						attributeValue("value", implicitExpressionAnnotation, ""),
						null /* TODO */,
						null
				);
				metadata.add(item);
			}

			try {
				FileObject resource = filer.createResource(StandardLocation.CLASS_OUTPUT, "", METADATA_FILE);
				try (OutputStream os = resource.openOutputStream()) {
					new JsonMarshaller().write(metadata, os);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Error while trying to write " + METADATA_FILE, e);
			}
		}

		return false;
	}

	/**
	 * Retrieve the value of an attribute in an annotation.
	 */
	@SuppressWarnings("unchecked")
	private <T> T attributeValue(String attributeName, AnnotationMirror annotation, T defaultValue) {
		Map<? extends ExecutableElement, ? extends AnnotationValue> elementValues = annotation.getElementValues();
		for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : elementValues.entrySet()) {
			if (entry.getKey().getSimpleName().contentEquals(attributeName)) {
				return (T) entry.getValue().getValue();
			}
		}
		return defaultValue;
	}

	/**
	 * Return the name of the synthetic property name to generate ({@literal foo}), asserting that the annotated
	 * field is indeed named {@literal fooExpression}.
	 */
	private String syntheticName(String name) {
		Assert.state(name.endsWith(EXPRESSION_SUFFIX), ImplicitExpression.class.getSimpleName() + " annotated field '"
				+ name + "' name does not end with " + EXPRESSION_SUFFIX);
		return name.substring(0, name.length() - EXPRESSION_SUFFIX.length());
	}

	/**
	 * Retrieve the annotation with the given type, using the mirror API.
	 */
	private AnnotationMirror getAnnotation(String type, List<? extends AnnotationMirror> annotationMirrors) {
		for (AnnotationMirror annotationMirror : annotationMirrors) {
			if (annotationMirror.getAnnotationType().toString().equals(type)) {
				return annotationMirror;
			}
		}
		throw new RuntimeException("No annotation of type " + type + " found in " + annotationMirrors);
	}
}
