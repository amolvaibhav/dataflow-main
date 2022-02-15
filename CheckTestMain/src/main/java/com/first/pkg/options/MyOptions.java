        package com.first.pkg.options;

        import org.apache.beam.sdk.options.*;
        import org.checkerframework.checker.initialization.qual.Initialized;
        import org.checkerframework.checker.nullness.qual.NonNull;
        import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public interface MyOptions extends PipelineOptions {

    @Validation.Required
    ValueProvider<String> getMainTable();
    void setMainTable(ValueProvider<String> mainTable);

    @Validation.Required
    ValueProvider<String> getInvalidTable();
    void setInvalidTable(ValueProvider<String> invalidTable);

    @Validation.Required
    ValueProvider<String> getInvalidSchemaTable();
    void setInvalidSchemaTable(ValueProvider<String> invalidSchemaTable);

    @Description("The full subscription Name")
    @Validation.Required
    ValueProvider<String> getFullSubName();
    void setFullSubName(ValueProvider<String> subName);

    @Validation.Required
    ValueProvider<String> getbqProject();
    void setbqProject(ValueProvider<String> bqProject);

    ValueProvider<String> getserviceURL();
    void setserviceURL(ValueProvider<String> serviceURL);

    ValueProvider<String> getPubsubProject();
    void setPubsubProject(ValueProvider<String> assetDataset);

    @Description("Path of the file to write to")
    @UnknownKeyFor @NonNull @Initialized
    String getTempLocation();

    void setTempLocation(String tempLocation);
}
