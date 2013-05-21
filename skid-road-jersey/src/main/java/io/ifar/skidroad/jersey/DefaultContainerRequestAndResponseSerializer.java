package io.ifar.skidroad.jersey;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sun.jersey.spi.container.ContainerRequest;
import io.ifar.skidroad.writing.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

/**
 * Serializes ContainerRequestAndResponse to JSON for flat-file storage.
 *
 * Future: configure what to include (e.g. only some headers)
 * Future: allow customizing class used to deserialize request entity bytes
 */
public class DefaultContainerRequestAndResponseSerializer implements Serializer<ContainerRequestAndResponse> {
    /**
     * The ContainerRequest and Response are pulled into an instance of this bean, and it is then serialized
     * into a single JSON String representing the log entry for this request/response pair.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public static class OutputBean {
        private static final Logger LOG = LoggerFactory.getLogger(OutputBean.class);
        public Object requestEntity;
        public String requestPath;
        public MultivaluedMap<String,String> requestHeaders;
        public MultivaluedMap<String,String> queryParameters;
        public int httpStatus;
        public Object responseEntity;

        public OutputBean(ContainerRequestAndResponse bean, ObjectMapper mapper) {
            ContainerRequest req = bean.getRequest();
            byte[] bytes = RequestEntityBytesCaptureFilter.getEntityBytes(req);
            if (bytes != null) {
                try {
                    this.requestEntity = mapper.readTree(bytes);
                } catch (IOException e) {
                    LOG.warn("Could not parse request entity to JSON.", e);
                }
            }
            this.requestPath = req.getPath();
            this.requestHeaders = req.getRequestHeaders();
            this.queryParameters = req.getQueryParameters();
            this.httpStatus = bean.getResponse().getStatus();
            this.responseEntity = bean.getResponse().getEntity();
        }
    }

    private final ObjectMapper objectMapper;

    public DefaultContainerRequestAndResponseSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(ContainerRequestAndResponse item) throws IOException {
        return objectMapper.writeValueAsString(new OutputBean(item, objectMapper));
    }
}
