package com.cloudera.frisch.randomdatagen.model.type;

import lombok.Getter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class NameField extends Field<String> {

    public class Name {
        @Getter
        String first_name;
        @Getter
        String country;
        @Getter
        Boolean unisex;
        @Getter
        Boolean female;
        @Getter
        Boolean male;

        public Name(String name, String country, String male, String female, String unisex ) {
            this.first_name = name;
            this.country = country;
            this.unisex = unisex.equalsIgnoreCase("true");
            this.male = male.equalsIgnoreCase("true");
            this.female = female.equalsIgnoreCase("true");
        }

        @Override
        public String toString() {
            return "Name{" +
                "name='" + first_name + '\'' +
                ", country='" + country + '\'' +
                ", unisex='" + unisex.toString() + '\'' +
                ", male='" + male.toString() + '\'' +
                ", female='" + female.toString() + '\'' +
                '}';
        }
    }

    private List<Name> nameDico;

    NameField(String name, Integer length, List<String> filters) {
        this.name = name;
        this.length = length;
        this.nameDico = loadNameDico();

        this.possibleValues = new ArrayList<>();

        filters.forEach(filterOnCountry -> {
            this.possibleValues.addAll(
                nameDico.stream().filter(n -> n.country.equalsIgnoreCase(filterOnCountry))
                    .map(n -> n.first_name)
                    .collect(Collectors.toList()));
        });
    }

    public String generateRandomValue() {
        return possibleValues.get(random.nextInt(possibleValues.size()));
    }

    private List<Name> loadNameDico() {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "dictionnaries/names.csv");
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .map(l -> {
                        String[] lineSplitted = l.split(";");
                        return new NameField.Name(lineSplitted[0], lineSplitted[1], lineSplitted[2], lineSplitted[3], lineSplitted[4]);
                    })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.warn("Could not load names-dico with error : " + e);
            return Collections.singletonList(new NameField.Name("Anonymous", "", "", "", ""));
        }
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public Put toHbasePut(String value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(String value, PartialRow partialRow) {
        partialRow.addString(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.STRING;
    }

    @Override
    public HivePreparedStatement toHive(String value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, value);
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
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createString();
    }

}