package io.ifar.skidroad.jersey.combined.serialize;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import io.ifar.skidroad.jersey.ContainerRequestAndResponse;
import io.ifar.skidroad.jersey.combined.capture.RequestEntityBytesCaptureFilter;
import io.ifar.skidroad.jersey.headers.CommonHeaderExtractors;
import io.ifar.skidroad.jersey.headers.RequestHeaderExtractor;
import io.ifar.skidroad.jersey.headers.ResponseHeaderExtractor;
import io.ifar.skidroad.writing.file.Serializer;
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
public class JSONContainerRequestAndResponseSerializer implements Serializer<ContainerRequestAndResponse> {

    private RequestHeaderExtractor requestHeaderExtractor = CommonHeaderExtractors.ALL_REQUEST_HEADERS;
    private ResponseHeaderExtractor responseHeaderExtractor = CommonHeaderExtractors.ALL_RESPONSE_HEADERS;

    /**
     * The ContainerRequest and Response are pulled into an instance of this bean, and it is then serialized
     * into a single JSON String representing the log entry for this request/response pair.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public class OutputBean {
        private final Logger LOG = LoggerFactory.getLogger(OutputBean.class);
        public Object requestEntity;
        public String requestPath;
        public MultivaluedMap<String,String> queryParameters;
        public int httpStatus;
        public Object responseEntity;

        @JsonIgnore
        private ContainerRequest request;

        @JsonIgnore
        private ContainerResponse response;

        public OutputBean(ContainerRequestAndResponse bean, ObjectMapper mapper) {
            this.request = bean.getRequest();
            this.response = bean.getResponse();

            byte[] bytes = RequestEntityBytesCaptureFilter.getEntityBytes(request);
            if (bytes != null) {
                try {
                    this.requestEntity = mapper.readTree(bytes);
                } catch (IOException e) {
                    LOG.warn("Could not parse request entity to JSON.", e);
                }
            }
            this.requestPath = request.getPath();
            this.queryParameters = request.getQueryParameters();
            this.httpStatus = bean.getResponse().getStatus();
            this.responseEntity = bean.getResponse().getEntity();
        }

        public MultivaluedMap<String, String> getRequestHeaders() {
            return requestHeaderExtractor.extract(request);
        }

        public MultivaluedMap<String, Object> getResponseHeaders() {
            return responseHeaderExtractor.extract(request, response);
        }
    }

    private final ObjectMapper objectMapper;

    public JSONContainerRequestAndResponseSerializer(ObjectMapper objectMapper) {
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT,false);
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(ContainerRequestAndResponse item) throws IOException {
        return objectMapper.writeValueAsString(new OutputBean(item, objectMapper));
    }

    public JSONContainerRequestAndResponseSerializer with(RequestHeaderExtractor requestHeaderExtractor) {
        this.requestHeaderExtractor = requestHeaderExtractor;
        return this;
    }

    public JSONContainerRequestAndResponseSerializer with(ResponseHeaderExtractor responseHeaderExtractor) {
        this.responseHeaderExtractor = responseHeaderExtractor;
        return this;
    }
}
