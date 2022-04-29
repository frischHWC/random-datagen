package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;


public class BytesField extends Field<byte[]> {

    BytesField(String name, Integer length, List<byte[]> possibleValues) {
        this.name = name;
        if (length == null || length < 1) {
            this.length = 20;
        } else {
            this.length = length;
        }
        this.possibleValues = possibleValues;
    }

    public byte[] generateRandomValue() {
        if (possibleValues.isEmpty()) {
            byte[] bytesArray = new byte[length];
            random.nextBytes(bytesArray);
            return bytesArray;
        } else {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        }
    }

    @Override
    public String toString(byte[] value) {
        return " " + name + " : " + DatatypeConverter.printHexBinary(value).toUpperCase() + " ;";
    }

    @Override
    public String toCSVString(byte[] value) {
        return DatatypeConverter.printHexBinary(value).toUpperCase() + ",";
    }


    /*
     Override if needed Field function to insert into special sinks
     */
    @Override
    public String toStringValue(byte[] value) {
        return DatatypeConverter.printHexBinary(value);
    }
    @Override
    public byte[] toCastValue(String value) {
        return value.getBytes();
    }

    @Override
    public Put toHbasePut(byte[] value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), value);
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(byte[] value, PartialRow partialRow) {
        partialRow.addBinary(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.BINARY;
    }

    @Override
    public HivePreparedStatement toHive(byte[] value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, DatatypeConverter.printHexBinary(value).toUpperCase());
        } catch (SQLException e) {
            logger.warn("Could not set value : " + DatatypeConverter.printHexBinary(value).toUpperCase() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "STRING";
    }

    @Override
    public String getGenericRecordType() {
        return "bytes";
    }

    @Override
    public Object toAvroValue(byte[] value) {
        return ByteBuffer.wrap(value);
    }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createBinary();
    }

}