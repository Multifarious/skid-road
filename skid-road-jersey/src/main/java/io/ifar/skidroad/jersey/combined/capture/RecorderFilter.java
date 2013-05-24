package io.ifar.skidroad.jersey.combined.capture;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import io.ifar.skidroad.jersey.ContainerRequestAndResponse;
import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicate;
import io.ifar.skidroad.jersey.predicate.response.ContainerResponsePredicate;
import io.ifar.skidroad.writing.WritingWorkerManager;

/**
 * A Jersey ContainerResponseFilter which bundles request/response pairs into {@link ContainerRequestAndResponse} beans
 * and passes them to Skid-Road for serialization.
 *
 * Use in conjunction with a {@link io.ifar.skidroad.jersey.combined.capture.RequestEntityBytesCaptureFilter} to request POST/PUT payloads as well.
 */
public class RecorderFilter implements ContainerResponseFilter{
    private final WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager;
    private final ContainerResponsePredicate predicate;

    /**
     * Construct a RecorderFilter that captures all requests.
     */
    public RecorderFilter(WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
        this.predicate = null;
        this.writingWorkerManager = writingWorkerManager;
    }

    /**
     * Construct a RecorderFilter that captures requests which match the provided predicate.
     */
    public RecorderFilter(final ContainerRequestPredicate predicate, WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
        this.predicate = new ContainerResponsePredicate() {
            @Override
            public boolean isMatch(ContainerRequest request, ContainerResponse response) {
                return predicate.isMatch(request);
            }
        };
        this.writingWorkerManager = writingWorkerManager;
    }

    /**
     * Construct a RecorderFilter that captures requests which match the provided predicate.
     */
    public RecorderFilter(ContainerResponsePredicate predicate, WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
        this.predicate = predicate;
        this.writingWorkerManager = writingWorkerManager;
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        //Future: consider introducing a predicate interface that takes response into account as well.
        if (predicate == null || predicate.isMatch(request, response)) {
            writingWorkerManager.record(System.currentTimeMillis(), new ContainerRequestAndResponse(request, response));
        }
        return response;
    }
}
