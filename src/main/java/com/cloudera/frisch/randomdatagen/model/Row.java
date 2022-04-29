package com.cloudera.frisch.randomdatagen.model;

import com.cloudera.frisch.randomdatagen.model.type.Field;
import com.cloudera.frisch.randomdatagen.sink.KafkaSink;
import com.cloudera.frisch.randomdatagen.sink.storedobjects.OzoneObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

/**
 * This class represents finest structure: a row
 * It should only be populated by Model when calling a generation of random data
 */
@Getter
@Setter
@NoArgsConstructor
@SuppressWarnings("unchecked")
public class Row<T extends Field> {
    private static final Logger logger = Logger.getLogger(Row.class);

    // A linkedHashMap is required to keep order in fields
    @Getter @Setter
    private LinkedHashMap<String, Object> values = new LinkedHashMap<>();
    @Getter @Setter
    private Model model;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f,o) -> {
            sb.append(f);
            sb.append(" : ");
            sb.append(model.getFieldFromName(f).toStringValue(o));
            sb.append("  ");
        });
        return sb.toString();
    }

    public String getPrimaryKeysValues(OptionsConverter.PrimaryKeys pk) {
        LinkedList<String> pkNames = (LinkedList<String>) model.getPrimaryKeys().get(pk);
        StringBuilder sb = new StringBuilder();
        pkNames.forEach(key -> sb.append(model.getFieldFromName(key).toStringValue(values.get(key))));
        return sb.toString();
    }

    public String toCSV() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f, o) -> sb.append(model.getFieldFromName(f).toCSVString(o)));
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String toJSON() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        values.forEach((f, o) -> sb.append(model.getFieldFromName(f).toJSONString(o)));
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" }");
        return sb.toString();
    }


    public Map.Entry<String, GenericRecord> toKafkaMessage(Schema schema) {
        GenericRecord genericRecordRow = new GenericData.Record(schema);
        values.forEach((f,o) -> genericRecordRow.put(f, model.getFieldFromName(f).toAvroValue(o)));
        return new AbstractMap.SimpleEntry<>(model.getPrimaryKeys().get(OptionsConverter.PrimaryKeys.KAFKA_MSG_KEY).toString(), genericRecordRow);
    }

    public Map.Entry<String, String> toKafkaMessageString(KafkaSink.MessageType messageType) {
        StringBuilder sb = new StringBuilder();
        if(messageType == KafkaSink.MessageType.CSV) {
            values.forEach((f, o) -> sb.append(model.getFieldFromName(f).toCSVString(o)));
        } else {
            values.forEach((f, o) -> sb.append(model.getFieldFromName(f).toJSONString(o)));
        }
        return new AbstractMap.SimpleEntry<>(getPrimaryKeysValues(OptionsConverter.PrimaryKeys.KAFKA_MSG_KEY), sb.toString());
    }

    public Put toHbasePut() {
        Put put = new Put(Bytes.toBytes(getPrimaryKeysValues(OptionsConverter.PrimaryKeys.HBASE_PRIMARY_KEY)));
        values.forEach((f, o) -> model.getFieldFromName(f).toHbasePut(o, put));
        return put;
    }

    public SolrInputDocument toSolRDoc() {
        SolrInputDocument doc = new SolrInputDocument();
        values.forEach((f, o) -> model.getFieldFromName(f).toSolrDoc(o, doc));
        return doc;
    }

    public OzoneObject toOzoneObject() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f, o) -> sb.append(model.getFieldFromName(f).toOzone(o)));
        // Bucket does not support upper case letter, so conversion to lower case is made
        return new OzoneObject(
                getPrimaryKeysValues(OptionsConverter.PrimaryKeys.OZONE_BUCKET).toLowerCase(),
                getPrimaryKeysValues(OptionsConverter.PrimaryKeys.OZONE_KEY),
                sb.toString()
        );
    }

    public Insert toKuduInsert(KuduTable table) {
        Insert insert = table.newInsert();
        PartialRow partialRow = insert.getRow();
        values.forEach((f,value) -> model.getFieldFromName(f).toKudu(value, partialRow));
        return insert;
    }

    public HivePreparedStatement toHiveStatement(HivePreparedStatement hivePreparedStatement){
        int i = 1;
        for(Map.Entry<String, Object> entry: values.entrySet()) {
            model.getFieldFromName(entry.getKey()).toHive(entry.getValue(),i,hivePreparedStatement);
            i++;
        }
        return hivePreparedStatement;
    }

    public GenericRecord toGenericRecord(Schema schema) {
        GenericRecord genericRecordRow = new GenericData.Record(schema);
        values.forEach((f,o) -> genericRecordRow.put(f, model.getFieldFromName(f).toAvroValue(o)));
        return genericRecordRow;
    }


    public void fillinOrcVector(int rowNumber, Map<String, ? extends ColumnVector> vectors) {
        vectors.forEach((field, cv) -> {
            switch (model.getFieldFromName(field).getClass().getSimpleName()) {
                case "IncrementLongField":
                case "LongField":
                case "TimestampField":
                    LongColumnVector longColumnVector = (LongColumnVector) cv;
                    longColumnVector.vector[rowNumber] = (long) values.get(field);
                    break;
                case "IncrementIntegerField":
                case "IntegerField":
                    LongColumnVector longColumnVectorInt = (LongColumnVector) cv;
                    longColumnVectorInt.vector[rowNumber] = Integer.toUnsignedLong((int) values.get(field));
                    break;
                case "FloatField":
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) cv;
                    doubleColumnVector.vector[rowNumber] = (float) values.get(field);
                    break;
                case "StringField":
                case "CountryField":
                case "StringAZField":
                case "NameField":
                case "EmailField":
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) cv;
                    String stringValue = (String) values.get(field);
                    bytesColumnVector.setVal(rowNumber, stringValue.getBytes(StandardCharsets.UTF_8));
                    break;
                case "BirthdateField":
                    BytesColumnVector bytesColumnVectorDate = (BytesColumnVector) cv;
                    LocalDate valueDate = (LocalDate) values.get(field);
                    bytesColumnVectorDate.setVal(rowNumber , valueDate.toString().getBytes(StandardCharsets.UTF_8));
                    break;
                case "BooleanField":
                    LongColumnVector longColumnVectorBoolean = (LongColumnVector) cv;
                    longColumnVectorBoolean.vector[rowNumber] = (boolean) values.get(field) ? 1L:0L;
                    break;
                case "BlobField":
                case "BytesField":
                case "HashMd5Field":
                    BytesColumnVector bytesColumnVectorBytes = (BytesColumnVector) cv;
                    bytesColumnVectorBytes.setVal(rowNumber, (byte[]) values.get(field));
                    break;
                default:
                    logger.warn("Cannot get types of Orc column: " + field + " as field is " + model.getFieldFromName(field).getClass().getSimpleName() );
            }
        });

    }

}
