package io.ifar.skidroad.jersey;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import io.ifar.skidroad.writing.WritingWorkerManager;

/**
 * Passes matching requests and their response to Skid-Road.
 *
 * Use in conjunction with a RequestEntityBytesCaptureFilter to capture POST/PUT payloads as well.
 */
public class SkidRoadFilter implements ContainerResponseFilter{
    private final WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager;
    private final ContainerRequestResponsePredicate predicate;

    /**
     * Construct a SkidRoadFilter that captures all requests.
     */
    public SkidRoadFilter(WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
        this.predicate = null;
        this.writingWorkerManager = writingWorkerManager;
    }

    /**
     * Construct a SkidRoadFilter that captures requests which match the provided predicate.
     */
    public SkidRoadFilter(final ContainerRequestPredicate predicate, WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
        this.predicate = new ContainerRequestResponsePredicate() {
            @Override
            public boolean isMatch(ContainerRequest request, ContainerResponse response) {
                return predicate.isMatch(request);
            }
        };
        this.writingWorkerManager = writingWorkerManager;
    }

    /**
     * Construct a SkidRoadFilter that captures requests which match the provided predicate.
     */
    public SkidRoadFilter(ContainerRequestResponsePredicate predicate, WritingWorkerManager<ContainerRequestAndResponse> writingWorkerManager) {
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
