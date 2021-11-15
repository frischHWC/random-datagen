package com.cloudera.frisch.randomdatagen.model.type;


import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.solr.common.SolrInputDocument;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;


/**
 * This abstract class describes a field with three characteristics: Name, Type, Length (which is optional)
 * Goal is also to describe how a field is rendered according to its type
 * Every new type added should extends this abstract class in a new Java Class (and override generateRandomValue())
 */
public abstract class Field<T> {

    static final Logger logger = Logger.getLogger(Field.class);
    Random random = new Random();

    @Getter
    @Setter
    public String name;

    @Getter
    @Setter
    public List<T> possibleValues;

    @Getter
    @Setter
    public LinkedHashMap<String, Integer> possible_values_weighted;

    @Getter
    @Setter
    public String hbaseColumnQualifier = "cq";

    // Default length is -1, if user does not provide a strict superior to 0 length,
    // each Extended field class should by default override it to a number strictly superior to 0
    @Getter
    @Setter
    public int length = -1;

    public abstract T generateRandomValue();

    @Override
    public String toString() {
        return "Class Type is " + this.getClass().getSimpleName() + " ; name : " + name + " ; hbase Column Qualifier : " + hbaseColumnQualifier
                + " ; Length : " + length;
    }

    public static String toString(List<Field> fieldList) {
        StringBuilder sb = new StringBuilder();
        sb.append("Fields :  [ ");
        sb.append(System.getProperty("line.separator"));
        fieldList.forEach(f -> {
            sb.append(" { ");
            sb.append(f.toString());
            sb.append(" }");
            sb.append(System.getProperty("line.separator"));
        });
        sb.append(" ] ");
        sb.append(System.getProperty("line.separator"));
        return sb.toString();
    }


    /**
     * Create the right instance of a field (i.e. String, password etc..) according to its type
     *
     * @param name            of the field that will be created
     * @param type            of the field, which is used to instantiate the right field
     * @param length          of the field, in could be null or -1, in this case, it will be ignored and default field length will be used
     * @param columnQualifier Hbase column qualifier if there is one
     * @return Field instantiated or null if type has not been recognized
     */
    public static Field instantiateField(String name, String type, Integer length, String columnQualifier, List<JsonNode> possibleValues, LinkedHashMap<String, Integer> possible_values_weighted) {
        if(name == null || name.isEmpty()) {
            throw new IllegalStateException("Name can not be null or empty for field: " + name);
        }
        if(type == null || type.isEmpty()) {
            throw new IllegalStateException("Type can not be null or empty for field: " + name);
        }

        // If length is not precised, it should be let as is (default is -1) and let each type handles it
        if (length == null || length < 1) {
            length = -1;
        }

        Field field;

        switch (type.toUpperCase()) {
            case "STRING":
                field = new StringField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()), possible_values_weighted);
                break;
            case "STRINGAZ":
                field = new StringAZField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()));
                break;
            case "INTEGER":
                field = new IntegerField(name, length, possibleValues.stream().map(JsonNode::asInt).collect(Collectors.toList()), possible_values_weighted);
                break;
            case "INCREMENT_INTEGER":
                field = new IncrementIntegerField(name, length, possibleValues.stream().map(JsonNode::asInt).collect(Collectors.toList()));
                break;
            case "BOOLEAN":
                field = new BooleanField(name, length, possibleValues.stream().map(JsonNode::asBoolean).collect(Collectors.toList()), possible_values_weighted);
                break;
            case "FLOAT":
                field = new FloatField(name, length, possibleValues.stream().map(j -> (float) j.asDouble()).collect(Collectors.toList()));
                break;
            case "LONG":
                field = new LongField(name, length, possibleValues.stream().map(JsonNode::asLong).collect(Collectors.toList()), possible_values_weighted);
                break;
            case "INCREMENT_LONG":
                field = new IncrementLongField(name, length, possibleValues.stream().map(JsonNode::asLong).collect(Collectors.toList()));
                break;
            case "TIMESTAMP":
                field = new TimestampField(name, length, possibleValues.stream().map(JsonNode::asLong).collect(Collectors.toList()));
                break;
            case "BYTES":
                field = new BytesField(name, length, possibleValues.stream().map(j -> j.asText().getBytes()).collect(Collectors.toList()));
                break;
            case "HASHMD5":
                field = new HashMd5Field(name, length, possibleValues.stream().map(j -> j.asText().getBytes()).collect(Collectors.toList()));
                break;
            case "BIRTHDATE":
                field = new BirthdateField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()));
                break;
            case "NAME":
                field = new NameField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()));
                break;
            case "COUNTRY":
                field = new CountryField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()));
                break;
            case "BLOB":
                field = new BlobField(name, length, possibleValues.stream().map(j -> j.asText().getBytes()).collect(Collectors.toList()));
                break;
            case "EMAIL":
                field = new EmailField(name, length, possibleValues.stream().map(JsonNode::asText).collect(Collectors.toList()));
                break;
            default:
                logger.warn("Type : " + type + " has not been recognized and hence will be ignored");
                return null;
        }

        // If hbase column qualifier is not precised, it should be let as is (default is "cq")
        if (columnQualifier != null && !columnQualifier.isEmpty()) {
            field.setHbaseColumnQualifier(columnQualifier);
        }

        return field;
    }

    /*
    Below functions could be redefined on each Field
    They provide generic Insertions needed
    Each time a new sink is added, a new function should be created here (or in each field)
     */

    public String toString(T value) {
        return " " + name + " : " + value.toString() + " ;";
    }

    public String toCSVString(T value) {
        return "\"" + value.toString() + "\",";
    }

    // This function needs to be overrided in each field
    public Put toHbasePut(T value, Put hbasePut) {
        //hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    public SolrInputDocument toSolrDoc(T value, SolrInputDocument doc) {
        doc.addField(name, value);
        return doc;
    }

    public String toOzone(T value) {
        return toString(value);
    }

    public PartialRow toKudu(T value, PartialRow partialRow) {
        partialRow.addObject(name, value);
        return partialRow;
    }

    public Type getKuduType() {
        return Type.BINARY;
    }

    public HivePreparedStatement toHive(T value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setObject(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " + value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    public String getHiveType() {
        return "BINARY";
    }

    public String getGenericRecordType() {
        return "string";
    }

    public Object toAvroValue(T value) {
        return value;
    }

    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createBinary();
    }

}
