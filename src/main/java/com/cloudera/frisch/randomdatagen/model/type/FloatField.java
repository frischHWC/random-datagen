package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.SQLException;
import java.util.List;

public class FloatField extends Field<Float> {

    FloatField(String name, Integer length, List<Float> possibleValues) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
    }

    public Float generateRandomValue() {
        return possibleValues.isEmpty() ? random.nextFloat() :
        possibleValues.get(random.nextInt(possibleValues.size()));
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(Float value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public HivePreparedStatement toHive(Float value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setFloat(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "FLOAT";
    }

    @Override
    public String getGenericRecordType() { return "float"; }

}
