package com.first.pkg;

import com.first.pkg.mapper.Mapper;
import com.first.pkg.mapper.Mappings;
import com.first.pkg.model.EnrichedObject;
import com.first.pkg.options.MyOptions;
import com.first.pkg.processor.MainProcessor;
import com.first.pkg.utility.Utility;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.ArrayList;
import java.util.List;


public class MainApplication {


    static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("countryCode").setType("STRING"));
        fields.add(new TableFieldSchema().setName("countryName").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sqlDate").setType("STRING"));
        fields.add(new TableFieldSchema().setName("actor1Name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sourceUrl").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }
    public static void main(String[] args) throws Exception {


        MyOptions options=PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        //MyOptions options=PipelineOptionsFactory.fromArgs(args).create();
        //PipelineOptions options=PipelineOptionsFactory.create();
        Pipeline p=Pipeline.create(options);

        PCollection<TableRow> read_table1 = p.apply("Read Table1", BigQueryIO.readTableRowsWithSchema().from("infoworks-326916:mydataset.gdelt_sample"));
        PCollection<TableRow> read_table2 = p.apply("Read Table1", BigQueryIO.readTableRowsWithSchema().from("infoworks-326916:mydataset.crosswalk_geocountrycodetohuman"));

        PCollection<TableRow> formattedResults = MainProcessor.joinEvents(read_table1, read_table2);

        formattedResults.apply("Write to BQ",BigQueryIO.writeTableRows().to("infoworks-326916:mydataset.BQWrite")
                        .withSchema(getSchema())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        p.run().waitUntilFinish();
    }
}
