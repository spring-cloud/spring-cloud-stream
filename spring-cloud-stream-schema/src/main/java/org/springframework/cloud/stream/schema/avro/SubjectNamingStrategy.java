package org.springframework.cloud.stream.schema.avro;

import org.apache.avro.Schema;

/**
 * @author David Kalosi
 */
public interface SubjectNamingStrategy {

    /**
     * Takes the Avro schema on input and returns the generated subject under which the schema should be registered.
     * @param schema
     * @return subject name
     */
    String toSubject(Schema schema);
}
