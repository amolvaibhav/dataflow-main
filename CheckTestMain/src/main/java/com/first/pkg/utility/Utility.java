package com.first.pkg.utility;

import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Utility {

    public static final TupleTag<String> PASS = new TupleTag<String>(){};
    public static final TupleTag<String> FAIL = new TupleTag<String>(){};

    public static String readJSONString(String fileName) throws IOException {
        String path="src/main/resources/"+fileName;
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }
}
