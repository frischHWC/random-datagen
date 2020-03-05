package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.sql.SQLException;
import java.util.List;

public class BooleanField extends Field<Boolean> {

    BooleanField(String name, Integer length, List<Boolean> possibleValues) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
    }

    public Boolean generateRandomValue() {
        return possibleValues.isEmpty() ? random.nextBoolean() :
        possibleValues.get(random.nextInt(possibleValues.size()));
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(Boolean value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(Boolean value, PartialRow partialRow) {
        partialRow.addBoolean(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.BOOL;
    }

    @Override
    public HivePreparedStatement toHive(Boolean value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setBoolean(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "BOOLEAN";
    }

    @Override
    public String getGenericRecordType() { return "boolean"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createBoolean();
    }

}
