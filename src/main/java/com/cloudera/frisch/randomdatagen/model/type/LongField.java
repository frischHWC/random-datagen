package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.SQLException;
import java.util.List;

public class LongField extends Field<Long> {

    LongField(String name, Integer length, List<Long> possibleValues) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
    }

    public Long generateRandomValue() {
        return possibleValues.isEmpty() ? random.nextLong() :
        possibleValues.get(random.nextInt(possibleValues.size()));
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(Long value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public HivePreparedStatement toHive(Long value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setLong(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "BIGINT";
    }

    @Override
    public String getGenericRecordType() { return "long"; }

}

