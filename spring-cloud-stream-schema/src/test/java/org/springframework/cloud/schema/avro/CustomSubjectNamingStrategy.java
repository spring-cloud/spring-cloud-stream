package org.springframework.cloud.schema.avro;

import org.apache.avro.Schema;
import org.springframework.cloud.stream.schema.avro.SubjectNamingStrategy;

/**
 * Created by david.kalosi on 10/23/2017.
 */
class CustomSubjectNamingStrategy implements SubjectNamingStrategy {

    public CustomSubjectNamingStrategy() {
    }

    @Override
    public String toSubject(Schema schema) {
        return schema.getFullName();
    }
}