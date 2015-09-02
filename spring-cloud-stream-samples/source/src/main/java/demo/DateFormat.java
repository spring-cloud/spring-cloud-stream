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

package demo;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.SimpleDateFormat;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/**
 * The annotated String must be a valid {@link java.text.SimpleDateFormat} pattern.
 *
 * @author Eric Bottard
 */
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER})
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = {DateFormat.DateFormatValidator.class})
public @interface DateFormat {

	String DEFAULT_MESSAGE = "";

	String message() default DEFAULT_MESSAGE;

	Class<?>[] groups() default {};

	Class<? extends Payload>[] payload() default {};


	/**
	 * Defines several {@link DateFormat} annotations on the same element.
	 *
	 * @see DateFormat
	 */
	@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER})
	@Retention(RUNTIME)
	@Documented
	@interface List {

		DateFormat[] value();
	}

	public static class DateFormatValidator implements ConstraintValidator<DateFormat, CharSequence> {

		private String message;

		@Override
		public void initialize(DateFormat constraintAnnotation) {
			this.message = constraintAnnotation.message();
		}

		@Override
		public boolean isValid(CharSequence value, ConstraintValidatorContext context) {
			if (value == null) {
				return true;
			}
			try {
				new SimpleDateFormat(value.toString());
			}
			catch (IllegalArgumentException e) {
				if (DEFAULT_MESSAGE.equals(this.message)) {
					context.disableDefaultConstraintViolation();
					context.buildConstraintViolationWithTemplate(e.getMessage()).addConstraintViolation();
				}
				return false;
			}
			return true;
		}
	}

}
