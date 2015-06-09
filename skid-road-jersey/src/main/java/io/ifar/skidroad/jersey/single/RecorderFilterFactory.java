package io.ifar.skidroad.jersey.single;

import com.google.common.base.Function;
import io.ifar.skidroad.jersey.ContainerRequestAndResponse;
import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicate;
import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicateBuilder;
import io.ifar.skidroad.jersey.predicate.response.ContainerResponsePredicate;
import io.ifar.skidroad.jersey.predicate.response.ContainerResponsePredicateBuilder;
import io.ifar.skidroad.recorder.BasicRecorder;
import io.ifar.skidroad.recorder.Recorder;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.glassfish.jersey.message.internal.ReaderWriter;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Cookie;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * Constructs Jersey ContainerRequestFilter objects which write data to Skid Road.
 *
 * The relevant pieces are:
 * 1) a predicate to determine whether the filter should run
 * 2) an "extractor" function to obtain the data to be recorded, given a request
 * 3) a "transformer" function to transform the data into the output format
 * 4) a function to determine the timestamp for the log entry. Usually request arrival time as determined by {@link RequestTimestampFilter}
 * 5) a Skid Road writer manager to consume the output
 *
 * Helper functions are provided to assemble default constellations of these pieces.
 */
public class RecorderFilterFactory {
    /**
     * A function to obtain the UUID of the request, as set by {@link UUIDGeneratorFilter}
     */
    public final static Function<ContainerRequestContext, String> EXTRACT_REQUEST_ID = new Function<ContainerRequestContext, String>() {
        @Nullable
        @Override
        public String apply(@Nullable ContainerRequestContext request) {
            return UUIDGeneratorFilter.getID();
        }
    };

    /**
     * A function to obtain the timestamp of the request, as set by {@link RequestTimestampFilter}
     */
    public final static Function<ContainerRequestContext, DateTime> EXTRACT_REQUEST_TIMESTAMP = new Function<ContainerRequestContext, DateTime>() {
        @Nullable
        @Override
        public DateTime apply(@Nullable ContainerRequestContext request) {
            return RequestTimestampFilter.getRequestDateTime();
        }
    };

    public final static Function<ContainerRequestAndResponse, DateTime> EXTRACT_REQUEST_TIMESTAMP_CRAR = new Function<ContainerRequestAndResponse, DateTime>() {
        @Nullable
        @Override
        public DateTime apply(@Nullable ContainerRequestAndResponse input) {
            return EXTRACT_REQUEST_TIMESTAMP.apply(input.getRequest());
        }
    };

    /**
     * A function to obtain the request body as a String
     */
    public final static Function<ContainerRequestContext, String> EXTRACT_REQUEST_BODY = new Function<ContainerRequestContext, String>() {
        @Nullable
        @Override
        public String apply(@Nullable ContainerRequestContext request) {
            if (request == null) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            InputStream in = request.getEntityStream();
            try {
                if(in.available() > 0) {
                    ReaderWriter.writeTo(in, out);

                    byte[] requestEntity = out.toByteArray();

                    request.setEntityStream(new ByteArrayInputStream(requestEntity));
                    return ReaderWriter.readFromAsString(new ByteArrayInputStream(requestEntity), request.getMediaType());
                } else {
                    return null;
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    };

    /**
     * A function to obtain the URI of the request
     */
    public final static Function<ContainerRequestContext, URI> EXTRACT_REQUEST_URI = new Function<ContainerRequestContext, URI>() {
        @Nullable
        @Override
        public URI apply(@Nullable ContainerRequestContext request) {
            return request == null ? null : request.getUriInfo().getRequestUri();
        }
    };

    /**
     * A function to obtain the HTTP Status code of the response
     */
    public final static Function<ContainerRequestAndResponse,Integer> EXTRACT_HTTP_STATUS = new Function<ContainerRequestAndResponse,Integer>() {
        @Nullable
        @Override
        public Integer apply(@Nullable ContainerRequestAndResponse requestAndResponse) {
            return requestAndResponse == null ? null : requestAndResponse.getResponse().getStatus();
        }
    };

    public static Function<ContainerRequestContext,List<String>> extractHeader(final String header) {
        return new Function<ContainerRequestContext, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable ContainerRequestContext request) {
                return request == null ? null : request.getHeaders().get(header);
            }
        };
    }

    public static Function<ContainerRequestContext,String> extractFirstHeader(final String header) {
        return new Function<ContainerRequestContext, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ContainerRequestContext request) {
                //Mimics Jersey 1 behavior.  May be more applicable to use request.getHeaders().getFirst(header);
                return request == null ? null : request.getHeaderString(header);
            }
        };
    }

    public static Function<ContainerRequestContext,String> extractCookie(final String cookieName) {
        return new Function<ContainerRequestContext, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ContainerRequestContext request) {
                if (request == null) {
                    return null;
                } else {
                    Cookie cookie = request.getCookies().get(cookieName);
                    if (cookie == null) {
                        return null;
                    } else {
                        return cookie.getValue();
                    }
                }
            }
        };
    }

    public static <F,T> ContainerRequestFilter build(
            final ContainerRequestPredicate predicate,
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestContext, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        final Recorder<ContainerRequestContext> recorder = new BasicRecorder<>(
                predicate,
                extractData,
                transform,
                determineTimestamp,
                writingWorkerManager,
                skipNulls
        );
        return new ContainerRequestFilter() {
            @Override
            public void filter(ContainerRequestContext requestContext) {
                recorder.record(requestContext);
            }
        };
    }

    public static <F,T> ContainerRequestFilter build(
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestContext, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(ContainerRequestPredicateBuilder.ALWAYS,extractData,transform,determineTimestamp,writingWorkerManager, skipNulls);
    }

    public static <F,T> ContainerRequestFilter build(
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(extractData,transform,RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP,writingWorkerManager, skipNulls);
    }

    public static <F,T> ContainerRequestFilter build(
            final ContainerRequestPredicate predicate,
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(predicate,extractData,transform,RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP,writingWorkerManager, skipNulls);
    }

    public static <F,T> ContainerRequestFilter build(
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestContext, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(ContainerRequestPredicateBuilder.ALWAYS,extractData,transform,determineTimestamp,writingWorkerManager, false);
    }

    public static <F,T> ContainerRequestFilter build(
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(extractData,transform,RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP,writingWorkerManager);
    }

    public static <F,T> ContainerRequestFilter build(
            final ContainerRequestPredicate predicate,
            final Function<ContainerRequestContext, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(predicate,extractData,transform,RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP,writingWorkerManager, false);
    }

    public static ContainerRequestFilter build(final List<ContainerRequestFilter> filters) {
        return new ContainerRequestFilter() {
            @Override
            public void filter(ContainerRequestContext requestContext) throws IOException {
                for (ContainerRequestFilter filter : filters) {
                    filter.filter(requestContext);
                }
            }
        };
    }

    //---- ContainerResponseFilter
    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final Recorder<ContainerRequestAndResponse> recorder)
    {
        return new ContainerResponseFilter() {
            @Override
            public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
                recorder.record(new ContainerRequestAndResponse(requestContext, responseContext));
            }
        };
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final ContainerResponsePredicate predicate,
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestAndResponse, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        final Recorder<ContainerRequestAndResponse> recorder = new BasicRecorder<>(
                predicate,
                extractData,
                transform,
                determineTimestamp,
                writingWorkerManager,
                skipNulls);
        return buildResponseFilter(recorder);
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestAndResponse, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            boolean skipNulls)
    {
        return buildResponseFilter(ContainerResponsePredicateBuilder.ALWAYS, extractData, transform, determineTimestamp, writingWorkerManager, skipNulls);
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return buildResponseFilter(extractData, transform, RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP_CRAR, writingWorkerManager, skipNulls);
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final ContainerResponsePredicate predicate,
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return buildResponseFilter(predicate, extractData, transform, RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP_CRAR, writingWorkerManager, skipNulls);
    }

        public static <F,T> ContainerResponseFilter buildResponseFilter(
            final ContainerResponsePredicate predicate,
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestAndResponse, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager)
        {
            return buildResponseFilter(predicate, extractData, transform, determineTimestamp, writingWorkerManager, false);
        }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final Function<ContainerRequestAndResponse, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return buildResponseFilter(ContainerResponsePredicateBuilder.ALWAYS, extractData, transform, determineTimestamp, writingWorkerManager, false);
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return buildResponseFilter(extractData, transform, RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP_CRAR, writingWorkerManager, false);
    }

    public static <F,T> ContainerResponseFilter buildResponseFilter(
            final ContainerResponsePredicate predicate,
            final Function<ContainerRequestAndResponse, F> extractData,
            final Function<F,T> transform,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return buildResponseFilter(predicate, extractData, transform, RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP_CRAR, writingWorkerManager, false);
    }
}
