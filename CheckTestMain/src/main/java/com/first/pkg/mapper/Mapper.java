package com.first.pkg.mapper;

import com.first.pkg.model.EnrichedObject;
import com.first.pkg.model.TableSchema;
import com.first.pkg.processor.MainProcessor;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import javafx.scene.control.Tab;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.kafka.common.protocol.types.Field;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Mapper  {

    private final static Logger log = Logger.getLogger(String.valueOf(Mapper.class));
    
    public static class MapMainInvalidFRecords extends SimpleFunction<EnrichedObject,TableRow>{

        String schema;

        public MapMainInvalidFRecords(String schema) {
            this.schema = schema;
        }

        @Override
        public TableRow apply(EnrichedObject input){
            TableRow row = new TableRow();
            try {

                Type token = new TypeToken<Map<String, List<TableSchema>>>() {
                }.getType();
                Map<String, List<TableSchema>> o = new Gson().fromJson(this.schema, token);
                List<TableSchema> fields = o.get("fields");
                //todo change to toString
                row.put(fields.get(0).getName(), input.getTagName());
                row.put(fields.get(1).getName(), input.getReason());
                row.put(fields.get(2).getName(), input.getValue());
                row.put(fields.get(3).getName(), input.getQuality());
                row.put(fields.get(4).getName(), input.getTimestamp());
                row.put(fields.get(5).getName(), DateTime.now().toString());


            }catch (Exception e){
                e.printStackTrace();
                log.warning("My message------"+e.getMessage());
            }
            return row;
        }
    }

    public static class MapMainRecords extends SimpleFunction<EnrichedObject,TableRow>{

        String schema;

        public MapMainRecords(String schema) {
            this.schema = schema;
        }

        @Override
        public TableRow apply(EnrichedObject input){
            TableRow row=new TableRow();
            Type token=new TypeToken<Map<String, List<TableSchema>>>(){}.getType();
            Map<String,List<TableSchema>> o = new Gson().fromJson(this.schema, token);
            List<TableSchema> fields = o.get("fields");
            String ts=input.getTimestamp();
           /* DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            Date parsedDate = null;
            try {
                sdf.setLenient(false);
                parsedDate = sdf.parse(ts);
                
            }catch (Exception e){
                log.warning(e.getMessage());
                e.printStackTrace();
            }
*/
            DateTime dt=DateTime.parse(ts);

            row.put(fields.get(0).getName(),input.getTagID());
            row.put(fields.get(1).getName(),input.getAssetID());
            row.put(fields.get(2).getName(),input.isAlarm());
            row.put(fields.get(3).getName(),input.getTagName());
            row.put(fields.get(4).getName(),input.getValue());
            row.put(fields.get(5).getName(),input.getQuality());
            row.put(fields.get(6).getName(),input.getTimestamp());
            row.put(fields.get(7).getName(),DateTime.now().toString());
            log.info("Element             "+row.toString());
            return row;
        }
    }

  public static class MapInvalidRecords extends SimpleFunction<String,TableRow>{

        String schema;

      public MapInvalidRecords(String schema) {
          this.schema = schema;
      }

      @Override
      public TableRow apply(String input){
          TableRow row=new TableRow();
          Type token=new TypeToken<Map<String, List<TableSchema>>>(){}.getType();
          Map<String,List<TableSchema>> o = new Gson().fromJson(this.schema, token);
          List<TableSchema> fields = o.get("fields");

          row.put(fields.get(0).getName(),input);
          row.put(fields.get(1).getName(),DateTime.now().toString());
          row.put(fields.get(2).getName(),"error");
          return row;
      }
  }
}
