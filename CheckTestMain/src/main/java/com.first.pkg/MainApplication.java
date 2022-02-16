package com.first.pkg;

import com.first.pkg.cache.CacheProcessor;
import com.first.pkg.cache.CacheProcessorOK;
import com.first.pkg.mapper.Mapper;
import com.first.pkg.mapper.Mappings;
import com.first.pkg.model.EnrichedObject;
import com.first.pkg.model.Message;
import com.first.pkg.options.MyOptions;
import com.first.pkg.processor.MainProcessor;
import com.first.pkg.utility.Utility;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.protocol.types.Field;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.checkerframework.checker.units.qual.K;
import org.joda.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Logger;

import static org.apache.beam.sdk.extensions.joinlibrary.Join.rightOuterJoin;

public class MainApplication {

    private final static Logger log = Logger.getLogger(String.valueOf(MainApplication.class));

    public static void main(String[] args){



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

        //PipelineOptions options=PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
        MyOptions options=PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().as(MyOptions.class);

        String mainTable=options.getMainTable().get();
        String invalidTable=options.getInvalidTable().get();
        String invalidSchemaTable=options.getInvalidSchemaTable().get();
        String fullSubName=options.getFullSubName().get();
        String serviceURL=options.getserviceURL().get();

        Pipeline p=Pipeline.create(options);
        //p.apply("read1", Create.of("{\"ABC\":[{\"q\":192,\"v\":542,\"ts\":\"2021-02-20 02:09:23.263 UTC\"}]}"))

        PCollection<Map<String,String>> myCacheMap = p.apply("Generate Sequence", GenerateSequence.from(0).withRate(1, Duration.standardMinutes(30)))
                .apply("Global Window", Window.<Long>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                        .discardingFiredPanes())
                .apply("Cache Processor", ParDo.of(new CacheProcessor(serviceURL)))
                ;//.apply("Constructing View", View.asSingleton());

        /*PCollection<Map<String,String>> myCacheMap= p.apply(JdbcIO.<Map<String, String>>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "com.mysql.cj.jdbc.Driver", "jdbc:mysql://34.131.120.173:3306/experiment")
                        .withUsername("myuser")
                        .withPassword("Classic@123"))
                .withQuery("select tag,data from tagdata")
                .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<Map<String, String>>() {
                    public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
                        Map<String,String> rowMap=new HashMap<>();
                        rowMap.put(resultSet.getString(1),resultSet.getString(2));
                        //return KV.of(resultSet.getString(1), resultSet.getString(2));
                        return rowMap;
                    }
                })
        );//.apply("Construct View",View.asMap());*/


        PCollectionTuple recTuple=p.apply("Read from PubSub",PubsubIO.readStrings().fromSubscription(fullSubName))
                        .apply("Verify Payload",ParDo.of(new MainProcessor.VerifyPayload()).withOutputTags(Utility.PASS, TupleTagList.of(Utility.FAIL)));


        PCollection<String> validRecords=recTuple.get(Utility.PASS);
        PCollection<String> inValidRecords=recTuple.get(Utility.FAIL);

        inValidRecords.apply("Convert TR for Invalid message", MapElements.via(new Mapper.MapInvalidRecords(invalidSchemaTableSchema)))
                        .apply("Write to BQ", BigQueryIO.writeTableRows().to(invalidSchemaTable).withJsonSchema(invalidSchemaTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        PCollection<KV<String, String>> feedback=validRecords.apply("Extract Message",ParDo.of(new MainProcessor.ExtractMessage()))
                .apply("Convert Map to KV", ParDo.of(new DoFn<Map<String, List<Message>>, KV<String, String>>() {

                    @ProcessElement
                    public void process(ProcessContext context){
                        for (Map.Entry<String,List<Message>> entry:context.element().entrySet()){
                            log.info("Message output:     "+KV.of(entry.getKey(),entry.getValue().toString()));
                                context.output(KV.of(entry.getKey(), entry.getValue().toString()));
                        }
                    }
                })).apply("window",
                        Window
                                .<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(50)))
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes());


                PCollection<KV<String ,String>> hello2= myCacheMap.apply("Convert Cache Map to Cache KV",ParDo.of(new DoFn<Map<String, String>, KV<String, String>>() {

                    @ProcessElement
                    public void process(ProcessContext context){
                        Map<String, String> element = context.element();

                        //Map<String, String> stringStringMap = context.sideInput(myCacheMap);
                        for (Map.Entry<String,String> entry:element.entrySet()){
                            log.info("Cache output:     "+KV.of(entry.getKey(), entry.getValue()));
                            context.output(KV.of(entry.getKey(), entry.getValue()));
                        }
                    }
                })).apply("window",
                        Window
                                .<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(50)))
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes());



        //PCollection<KV<String, CoGbkResult>> coGBKResult = KeyedPCollectionTuple.of(Utility.MESSAGE, feedback).and(Utility.CACHE, hello2).apply(CoGroupByKey.create());


        //Using rightOuterJoin to bring out the null values from the cache dataset. For Equi-Join, uncomment line 161
        //nullValue returns a default value for the null values from the cache dataset.
        PCollection<KV<String,KV<String,String>>> kvRightJoinPCollection= Join.rightOuterJoin(hello2, feedback,"0");
        //PCollection<KV<String, KV<String, String>>> kvpJoinCollection = Join.innerJoin(hello2, feedback);


        PCollection<List<EnrichedObject>> processJoins= kvRightJoinPCollection.apply("Process Joins", ParDo.of(new DoFn<KV<String, KV<String, String>>, List<EnrichedObject>>() {

            @ProcessElement
            public void process(ProcessContext context) {
                KV<String, KV<String, String>> element = context.element();
                log.info("Main Key:   " + element.getKey());
                log.info("Second Key:  " + element.getValue().getKey());
                log.info("Main Last Value:   " + element.getValue().getValue());
                String tag = element.getKey();
                String cachedValue = element.getValue().getKey();
                String strMessageArray = element.getValue().getValue();
                JSONArray jsonMessageArray = new JSONArray(strMessageArray);
                List<EnrichedObject> finalEnrichedObjectList = new ArrayList<>();
                if (element.getValue().getKey().equals("0")) {
                    for (int j=0;j<jsonMessageArray.length();j++){
                        Message messageElement = new Gson().fromJson(jsonMessageArray.get(j) + "", Message.class);
                        log.info("Empty Cache Found. Message:  "+messageElement.toString());
                        EnrichedObject enrichedObject=new EnrichedObject();
                        enrichedObject.setTagName(element.getKey());
                        enrichedObject.setQuality(messageElement.getQuality());
                        enrichedObject.setValue(messageElement.getValue());
                        enrichedObject.setTimestamp(messageElement.getTs());
                        finalEnrichedObjectList.add(enrichedObject);
                    }
                } else{
                    for (int i = 0; i < jsonMessageArray.length(); i++) {
                        Message messageElement = new Gson().fromJson(jsonMessageArray.get(i) + "", Message.class);
                        log.info("Timestamp: " + messageElement.ts + "  Quality:  " + messageElement.getQuality() + " Value:  " + messageElement.getValue());

                        EnrichedObject cachedObject = new Gson().fromJson(element.getValue().getKey(), EnrichedObject.class);
                        EnrichedObject enrichedObject = new EnrichedObject(tag, messageElement.getQuality(), messageElement.getValue(), messageElement.getTs());
                        enrichedObject.setTagID(cachedObject.getTagID());
                        enrichedObject.setAssetID(cachedObject.getAssetID());
                        enrichedObject.setAlarm(cachedObject.isAlarm());
                        finalEnrichedObjectList.add(enrichedObject);
                    }
            }
                context.output(finalEnrichedObjectList);
            }
        }));

        //.apply("Lookup Cache",ParDo.of(new MainProcessor.LookupCache(myCacheMap)).withSideInputs(myCacheMap))
        PCollectionList<EnrichedObject> verifiedObject = processJoins.apply("Flatten List", Flatten.iterables())
                .apply("Final Cache Verification", Partition.of(2, new MainProcessor.FinalVerifyObject()));

        PCollection<EnrichedObject> vRecords = verifiedObject.get(0);
        PCollection<EnrichedObject> iRecords = verifiedObject.get(1);

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
