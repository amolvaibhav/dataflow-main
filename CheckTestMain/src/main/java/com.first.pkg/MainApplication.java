package com.first.pkg;

import com.first.pkg.mapper.Mapper;
import com.first.pkg.mapper.Mappings;
import com.first.pkg.model.EnrichedObject;
import com.first.pkg.processor.MainProcessor;
import com.first.pkg.utility.Utility;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

public class MainApplication {

    public static void main(String[] args){

        String mainTable="infoworks-326916:mydataset.fact_main";
        String invalidTable="infoworks-326916:mydataset.error_cache";
        String invalidSchemaTable="infoworks-326916:mydataset.invalid_schema";

        String mainTableSchema="";
        String invalidTableSchema="";
        String invalidSchemaTableSchema="";
        try{
            mainTableSchema=Utility.readJSONString("main_schema.json");
            invalidTableSchema=Utility.readJSONString("invalida_schema.json");
            invalidSchemaTableSchema=Utility.readJSONString("invalid_table_schema.json");
        }catch (Exception e){
            e.printStackTrace();
        }

        PipelineOptions options=PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
        Pipeline p=Pipeline.create(options);
        //p.apply("read1", Create.of("{\"ABC\":[{\"q\":192,\"v\":542,\"ts\":\"2021-02-20 02:09:23.263 UTC\"}]}"))



        PCollectionTuple recTuple=p.apply("Read from PubSub",PubsubIO.readStrings().fromSubscription("projects/infoworks-326916/subscriptions/real-time-data-sub"))
                        .apply("Verify Payload",ParDo.of(new MainProcessor.VerifyPayload()).withOutputTags(Utility.PASS, TupleTagList.of(Utility.FAIL)));


        PCollection<String> validRecords=recTuple.get(Utility.PASS);
        PCollection<String> inValidRecords=recTuple.get(Utility.FAIL);

        inValidRecords.apply("Convert TR for Invalid message", MapElements.via(new Mapper.MapInvalidRecords(invalidSchemaTableSchema)))
                        .apply("Write to BQ", BigQueryIO.writeTableRows().to(invalidSchemaTable).withJsonSchema(invalidSchemaTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PCollectionList<EnrichedObject> feedback=validRecords.apply("Extract Message",ParDo.of(new MainProcessor.ExtractMessage()))
                        .apply("Lookup Cache",ParDo.of(new MainProcessor.LookupCache()))
                        .apply("Flatten List",Flatten.iterables())
                        .apply("Final Cache Verification",Partition.of(2, new MainProcessor.FinalVerifyObject()));

        PCollection<EnrichedObject> vRecords = feedback.get(0);
        PCollection<EnrichedObject> iRecords = feedback.get(1);

        vRecords.apply("Convert TR for Main Table",MapElements.via(new Mapper.MapMainRecords(mainTableSchema)))
                .apply("BQ Main Write",BigQueryIO.writeTableRows().to(mainTable).withJsonSchema(mainTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        iRecords.apply("Convert TR for Cache lookup Error Records",MapElements.via(new Mapper.MapMainInvalidFRecords(invalidTableSchema)))
                .apply("Write to BQ for Cache Error Records",BigQueryIO.writeTableRows().to(invalidTable).withJsonSchema(invalidTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
                        ;


        p.run().waitUntilFinish();
    }
}
