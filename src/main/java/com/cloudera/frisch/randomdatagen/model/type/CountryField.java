package com.cloudera.frisch.randomdatagen.model.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.jdbc.HivePreparedStatement;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class CountryField extends Field<String> {

    private List<String> countryDico;

    CountryField(String name, Integer length, List<String> possibleValues) {
        this.name = name;
        this.length = length;
        this.possibleValues = possibleValues;
        this.countryDico = possibleValues.isEmpty() ? loadCountryDico() : possibleValues;
    }

    public String generateRandomValue() {
        return countryDico.get(random.nextInt(countryDico.size()));
    }

    private List<String> loadCountryDico() {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "dictionnaries/country-dico.txt");
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.warn("Could not load country-dico with error : " + e);
            return Collections.singletonList("World");
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
    public HivePreparedStatement toHive(String value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setString(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " + value + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "STRING";
    }

    @Override
    public String getGenericRecordType() { return "string"; }



}