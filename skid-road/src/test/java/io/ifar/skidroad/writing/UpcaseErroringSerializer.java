package io.ifar.skidroad.writing;

import io.ifar.skidroad.writing.file.Serializer;

import java.io.IOException;

public class UpcaseErroringSerializer implements Serializer<String> {
    public int count = 0;
    public boolean throwErrors = false;

    @Override
    public String serialize(String item) throws IOException {
        count++;
        if (throwErrors)
            throw new IOException("fake serialization error");
        return item.toUpperCase();
    }
}
