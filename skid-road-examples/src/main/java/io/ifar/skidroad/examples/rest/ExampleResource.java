package io.ifar.skidroad.examples.rest;

import io.ifar.goodies.Pair;
import io.ifar.skidroad.writing.WritingWorkerManager;

import javax.ws.rs.*;

/**
 * Post to this resource with:
 * curl -X POST -H "Content-Type: application/json" -d '{"radius":1.2, "with_gold":true}' http://localhost:8080/orders
 *
 * Request and response will be logged because of applied container filters. Alternate approach would be for this
 * resource (or a collaborator) to have a direct reference to a Skid Road WritingWorkerManager and explicitly pass
 * data into it.
 */
@Path("/")
public class ExampleResource {
    private final WritingWorkerManager<Pair<RainbowRequest, RainbowRequestResponse>> writingWorkerManager;

    public ExampleResource(WritingWorkerManager<Pair<RainbowRequest, RainbowRequestResponse>> writingWorkerManager) {
        this.writingWorkerManager = writingWorkerManager;
    }

    @POST
    @Path("orders")
    @Consumes("application/json")
    @Produces("application/json")
    public RainbowRequestResponse requestRainbow(RainbowRequest req, @QueryParam("fav_color") String favoriteColor) {
        String rainbowColor;
        if (favoriteColor == null) {
            rainbowColor = "all";
        } else {
            rainbowColor = favoriteColor;
        }
        RainbowRequestResponse resp = new RainbowRequestResponse(true,rainbowColor);
        writingWorkerManager.record(new Pair<>(req,resp));

        return resp;
    }
}
