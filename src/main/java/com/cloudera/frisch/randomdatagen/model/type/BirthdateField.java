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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;


public class BirthdateField extends Field<LocalDate> {

    BirthdateField(String name, Integer length, List<String> possibleValues) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/MM/yyyy");
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues.stream().map(p -> LocalDate.parse(p, formatter)).collect(Collectors.toList());
    }

    /**
     * Generates a random birth date between 1910 & 2020
     * @return
     */
    public LocalDate generateRandomValue() {
        if(possibleValues.isEmpty()) {
            long minDay = LocalDate.of(1910, 1, 1).toEpochDay();
            long maxDay = LocalDate.of(2020, 1, 1).toEpochDay();
            long randomDay = minDay + random.nextInt((int) maxDay - (int) minDay);
            return LocalDate.ofEpochDay(randomDay);
        } else {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        }
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(LocalDate value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value.toString()));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(LocalDate value, PartialRow partialRow) {
        partialRow.addString(name, value.toString());
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.STRING;
    }

    @Override
    public HivePreparedStatement toHive(LocalDate value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, value.toString());
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "STRING";
    }

    @Override
    public String getGenericRecordType() { return "string"; }

    @Override
    public Object toAvroValue(LocalDate value) { return value.toString(); }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createString();
    }

}