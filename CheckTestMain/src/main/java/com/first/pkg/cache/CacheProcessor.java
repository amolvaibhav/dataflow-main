package com.first.pkg.cache;

import com.first.pkg.options.MyOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
//import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;

public class CacheProcessor extends PTransform<PCollection<String>, PCollectionTuple> {

    private final MyOptions options;

    public CacheProcessor(MyOptions options) {
        this.options = options;
    }


    @Override
    public PCollectionTuple expand(PCollection<String> input) {

        //input.apply("Invoke UDF",FailsafeJavascriptUdf)
        return null;
    }
}
