package org.springframework.cloud.stream.schema.avro;

import org.apache.avro.Schema;

/**
 * @author David Kalosi
 */
public class DefaultSubjectNamingStrategy implements SubjectNamingStrategy {

    @Override
    public String toSubject(Schema schema) {
        return schema.getName().toLowerCase();
    }
}
