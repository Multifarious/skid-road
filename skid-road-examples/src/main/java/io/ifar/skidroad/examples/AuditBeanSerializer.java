package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ifar.skidroad.writing.Serializer;

import java.io.IOException;

/**
 * Serializes AuditBean for flat-file storage. JSON here for simplicity, but
 * could be CSV, XML, etc.
 */
public class AuditBeanSerializer implements Serializer<AuditBean> {
    private final ObjectMapper objectMapper;

    public AuditBeanSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(AuditBean item) throws IOException {
        return objectMapper.writeValueAsString(item);
    }
}
