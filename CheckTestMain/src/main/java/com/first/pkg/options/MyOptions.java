package com.first.pkg.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface MyOptions extends PipelineOptions {
    @Validation.Required
    @Description("Gcs path to javascript udf source")
    ValueProvider<String> getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(ValueProvider<String> javascriptTextTransformGcsPath);

    @Validation.Required
    @Description("UDF Javascript Function Name")
    ValueProvider<String> getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(
            ValueProvider<String> javascriptTextTransformFunctionName);
}
