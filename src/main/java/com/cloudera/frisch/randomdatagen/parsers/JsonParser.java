package com.cloudera.frisch.randomdatagen.parsers;


import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Goal of this class is to be able to parse a codified file JSON and render a model based on it
 */
@SuppressWarnings("unchecked")
public class JsonParser<T extends Field> implements Parser {

    private JsonNode root;

    public JsonParser(String jsonFilePath) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            logger.info("Model used is from Json file : " + jsonFilePath);
            root = mapper.readTree(new File(jsonFilePath));
            logger.debug("JSON file content is :" + root.toPrettyString());
        } catch (IOException e) {
            logger.error("Could not read JSON file: " + jsonFilePath + ", please verify its structure, error is : ", e);
        } catch (NullPointerException e) {
            logger.error("Model file has not been found at : " + jsonFilePath + " , please verify it exists, error is: ", e);
            System.exit(1);
        }
    }

    /**
     * Creates a model and populate it by reading the JSON file provided as argument of the constructor
     *
     * @return Model instantiated and populated
     */
    public Model renderModelFromFile() {

        Iterator<JsonNode> pksIterator = root.findPath("Primary_Keys").elements();
        Map<String, List<String>> pks = new HashMap<>();

        while (pksIterator.hasNext()) {
            addPrimaryKeyToHashMap(pks, pksIterator.next());
        }

        Iterator<JsonNode> tablesIterator = root.findPath("Table_Names").elements();
        Map<String, String> tbs = new HashMap<>();

        while (tablesIterator.hasNext()) {
            addTableNameToHashMap(tbs, tablesIterator.next());
        }

        Iterator<JsonNode> optionsIterator = root.findPath("Options").elements();
        Map<String, String> opsMap = new HashMap<>();

        while (optionsIterator.hasNext()) {
            addOptionToHashMap(opsMap, optionsIterator.next());
        }

        Iterator<JsonNode> fieldsIterator = root.findPath("Fields").elements();
        LinkedList<T> fields = new LinkedList<>();

        Map<String, String> hbaseFamilyColsMap = new HashMap<>();
        try {
            hbaseFamilyColsMap = mapColNameToColQual(opsMap.get("HBASE_COLUMN_FAMILIES_MAPPING"));
        } catch (Exception e) {
            logger.warn("Could not get any column qualifier, defaulting to 'cq', check you are not using HBase ");
        }

        while (fieldsIterator.hasNext()) {
            T field = getOneField(fieldsIterator.next(), hbaseFamilyColsMap);
            if (field != null) {
                fields.add(field);
            }
        }

        return new Model(fields, pks, tbs, opsMap);
    }



    /**
     * Get one field from a Json node by instantiating the right type of field and return it
     * Note that if no length is precised in the JSON, it is automatically handled
     *
     * @param jsonField
     * @return
     */
    private T getOneField(JsonNode jsonField, Map<String, String> opsMap) {
        Integer length;
        try {
            length = jsonField.get("length").asInt();
        } catch (NullPointerException e) {
            length = null;
        }

        JsonNode possibleValuesArray = jsonField.get("possible_values");
        List<JsonNode> possibleValues = new ArrayList<>();
        try {
            if(possibleValuesArray.isArray()) {
                for(JsonNode possibleValue: possibleValuesArray) {
                    possibleValues.add(possibleValue);
                }
            }
        } catch (NullPointerException e) {
            possibleValues = Collections.emptyList();
        }

        return (T) Field.instantiateField(jsonField.get("name").asText(), jsonField.get("type").asText(), length,
                opsMap.get(jsonField.get("name").asText()), possibleValues);
    }

    private Map<String, String> mapColNameToColQual(String mapping) {
        Map<String, String> hbaseFamilyColsMap = new HashMap<>();
        for (String s : mapping.split(";")) {
            String cq = s.split(":")[0];
            for (String c : s.split(":")[1].split(",")) {
                    hbaseFamilyColsMap.put(c, cq);
            }
        }
        return hbaseFamilyColsMap;
    }

    private void addOptionToHashMap(Map<String, String> options, JsonNode jsonOptions) {
        Iterator<String> jsonNodeIterator = jsonOptions.fieldNames();
        while(jsonNodeIterator.hasNext()) {
            String nodeName = jsonNodeIterator.next();
            options.put(nodeName, jsonOptions.get(nodeName).asText());
        }
    }


    private void addPrimaryKeyToHashMap(Map<String, List<String>> pks, JsonNode jsonPk) {
        Iterator<String> jsonNodeIterator = jsonPk.fieldNames();
        while(jsonNodeIterator.hasNext()) {
            String nodeName = jsonNodeIterator.next();
           pks.put(nodeName, Arrays.asList(jsonPk.get(nodeName).asText().split("\\s*,\\s*")));
        }
    }

    private void addTableNameToHashMap(Map<String, String> tbs, JsonNode jsonTb) {
        Iterator<String> jsonNodeIterator = jsonTb.fieldNames();
        while(jsonNodeIterator.hasNext()) {
            String nodeName = jsonNodeIterator.next();
            tbs.put(nodeName, jsonTb.get(nodeName).asText());
        }
    }
}
