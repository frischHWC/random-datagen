package com.cloudera.frisch.randomdatagen.model;

import com.cloudera.frisch.randomdatagen.model.type.Field;
import com.cloudera.frisch.randomdatagen.sink.objects.OzoneObject;
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

    // A linkedHashMap is required to keep order in fields (which should be the same than fields from Model)
    @Getter @Setter
    private LinkedHashMap<T, Object> values;
    @Getter @Setter
    private Map<OptionsConverter.PrimaryKeys, Object> pksValues;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f,o) -> sb.append(f.toString(o)));
        sb.append(System.getProperty("line.separator"));
        sb.append(" Primary Keys values : ");
        pksValues.forEach((pk, o) -> {
            sb.append(pk);
            sb.append(" = ");
            sb.append(o.toString());
            sb.append(" ; ");
        });
        return sb.toString();
    }

    /**
     * Population of primary key values is made using Map passed (coming from Model class)
     * If there is only one field as PK, its type is kept, if there are more than one, a String type is used
     *
     * @param primaryKeys map of primary keys with list of fields
     */
    void populatePksValues(Map<OptionsConverter.PrimaryKeys, List<T>> primaryKeys) {
        Map<OptionsConverter.PrimaryKeys, Object> primaryKeysObjectMap = new HashMap<>();
        primaryKeys.forEach((pk, fields) -> {
            if (fields.size() == 1) {
                primaryKeysObjectMap.put(pk, values.get(fields.get(0)));
            } else {
                StringBuilder sb = new StringBuilder();
                fields.forEach(field -> sb.append(values.get(field)));
                primaryKeysObjectMap.put(pk, sb.toString());
            }
        });
        pksValues = primaryKeysObjectMap;
    }

    public String toCSV() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f, o) -> sb.append(f.toCSVString(o)));
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }


    public Map.Entry<String, GenericRecord> toKafkaMessage(Schema schema) {
        GenericRecord genericRecordRow = new GenericData.Record(schema);
        values.forEach((f,o) -> genericRecordRow.put(f.name, f.toAvroValue(o)));
        return new AbstractMap.SimpleEntry<>(pksValues.get(OptionsConverter.PrimaryKeys.KAFKA_MSG_KEY).toString(), genericRecordRow);
    }

    public Put toHbasePut() {
        Put put = new Put(Bytes.toBytes(pksValues.get(OptionsConverter.PrimaryKeys.HBASE_PRIMARY_KEY).toString()));
        values.forEach((f, o) -> f.toHbasePut(o, put));
        return put;
    }

    public SolrInputDocument toSolRDoc() {
        SolrInputDocument doc = new SolrInputDocument();
        values.forEach((f, o) -> f.toSolrDoc(o, doc));
        return doc;
    }

    public OzoneObject toOzoneObject() {
        StringBuilder sb = new StringBuilder();
        values.forEach((f, o) -> sb.append(f.toOzone(o)));
        // Bucket does not support upper case letter, so conversion to lower case is made
        return new OzoneObject(
                pksValues.get(OptionsConverter.PrimaryKeys.OZONE_BUCKET).toString().toLowerCase(),
                pksValues.get(OptionsConverter.PrimaryKeys.OZONE_KEY).toString(),
                sb.toString()
        );
    }

    public Insert toKuduInsert(KuduTable table) {
        Insert insert = table.newInsert();
        PartialRow partialRow = insert.getRow();
        values.forEach((field,value) -> field.toKudu(value, partialRow));
        return insert;
    }

    public HivePreparedStatement toHiveStatement(HivePreparedStatement hivePreparedStatement){
        int i = 1;
        for(Map.Entry<? extends Field,Object> entry: values.entrySet()) {
            entry.getKey().toHive(entry.getValue(),i,hivePreparedStatement);
            i++;
        }
        return hivePreparedStatement;
    }

    public GenericRecord toGenericRecord(Schema schema) {
        GenericRecord genericRecordRow = new GenericData.Record(schema);
        values.forEach((f,o) -> genericRecordRow.put(f.name, f.toAvroValue(o)));
        return genericRecordRow;
    }


    public void fillinOrcVector(int rowNumber, Map<T, ? extends ColumnVector> vectors) {
        vectors.forEach((field, cv) -> {
            switch (field.getClass().getSimpleName()) {
                case "LongField":
                case "TimestampField":
                    LongColumnVector longColumnVector = (LongColumnVector) cv;
                    longColumnVector.vector[rowNumber] = (long) values.get(field);
                    break;
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
                    logger.warn("Cannot get types of Orc column: " + field.getName() + " as field is " + field.getClass().getSimpleName() );
            }
        });

    }

}
