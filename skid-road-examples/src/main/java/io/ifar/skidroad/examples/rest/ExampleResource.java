package io.ifar.skidroad.examples.rest;

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
        return new RainbowRequestResponse(true,rainbowColor);
    }
}
