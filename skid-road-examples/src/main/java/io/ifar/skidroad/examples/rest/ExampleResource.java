package io.ifar.skidroad.examples.rest;

import io.ifar.skidroad.examples.AuditBean;
import io.ifar.skidroad.writing.WritingWorkerManager;

import javax.ws.rs.*;

/**
 * Post to this resource with:
 * curl -X POST -H "Content-Type: application/json" -d '{"radius":1.2, "withGold":true}' http://localhost:8080/orders
 */
@Path("/")
public class ExampleResource {

    private WritingWorkerManager<AuditBean> writingWorkerManager;

    public ExampleResource(WritingWorkerManager<AuditBean> writingWorkerManager) {
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
        RainbowRequestResponse response = new RainbowRequestResponse(true,rainbowColor);
        AuditBean audit = new AuditBean(req, favoriteColor, response);
        writingWorkerManager.record(System.currentTimeMillis(), audit);
        return response;
    }
}
