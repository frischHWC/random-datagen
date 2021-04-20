package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.sql.SQLException;
import java.util.List;

public class IntegerField extends Field<Integer> {

    IntegerField(String name, Integer length, List<Integer> possibleValues) {
        this.name = name;
        if(length==null || length==-1) {
            this.length = Integer.MAX_VALUE;
        } else {
            this.length = length;
        }
        this.possibleValues = possibleValues;
    }

    public Integer generateRandomValue() {
        return possibleValues.isEmpty() ? random.nextInt(length) :
                possibleValues.get(random.nextInt(possibleValues.size()));
    }

    /*
    Override if needed Field function to insert into special sinks
    */
    @Override
    public Put toHbasePut(Integer value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public HivePreparedStatement toHive(Integer value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setInt(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "INT";
    }

    @Override
    public String getGenericRecordType() { return "int"; }

}
