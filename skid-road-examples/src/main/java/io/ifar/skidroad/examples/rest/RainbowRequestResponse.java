package io.ifar.skidroad.examples.rest;

/**
 */
public class RainbowRequestResponse {
    private final boolean requestGranted;
    private final String rainbowColor;

    public boolean isRequestGranted() {
        return requestGranted;
    }

    public String getRainbowColor() {
        return rainbowColor;
    }

    public RainbowRequestResponse(boolean requestGranted, String rainbowColor) {

        this.requestGranted = requestGranted;
        this.rainbowColor = rainbowColor;
    }
}
