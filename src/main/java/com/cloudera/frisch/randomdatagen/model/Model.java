package com.cloudera.frisch.randomdatagen.model;


import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents a model, that will be used to describe:
 * - all fields
 * - Primary keys & fields to bucket on
 * - Tables names
 * - Other options if needed
 * This class describes also how to generate random data
 * It also describe how to initialize certain systems for that model (i.e. table creation)
 */
@Getter
@Setter
@SuppressWarnings("unchecked")
public class Model<T extends Field> {

    private static final Logger logger = Logger.getLogger(Model.class);

    // LinkedList is enforced to keep order in list of fields (required when generating requests and filling them after)
    @Getter @Setter
    private LinkedList<T> fields;
    @Getter @Setter
    private Map<OptionsConverter.PrimaryKeys, List<T>> primaryKeys;
    @Getter @Setter
    private Map<OptionsConverter.TableNames, String> tableNames;
    @Getter @Setter
    private Map<OptionsConverter.Options, Object> options;

    /**
     * Constructor that initializes the model and populates it completely
     * (it only assumes that Field objects are already created, probably using instantiateField() from Field class)
     *
     * @param fields      list of fields already instantiated
     * @param primaryKeys map of options of PKs to list of PKs
     * @param tableNames  map of options of Table names to their names
     * @param options     map of other options as String, String
     */
    public Model(LinkedList<T> fields, Map<String, List<String>> primaryKeys, Map<String, String> tableNames, Map<String, String> options) {
        this.fields = fields;
        this.primaryKeys = convertPrimaryKeys(primaryKeys);
        this.tableNames = convertTableNames(tableNames);
        this.options = convertOptions(options);

        // Using Options passed, fields should be updated to take into account extra options passed
        setupFieldHbaseColQualifier((Map<T, String>) this.options.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING));

        if (logger.isDebugEnabled()) {
            logger.debug("Model created is : " + toString());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Field.toString(fields);
        sb.append(fields.toString());
        sb.append("Primary Keys : [");
        primaryKeys.forEach((pk, fl) -> {
            sb.append(pk);
            sb.append(" : { ");
            // Field.toString(fl)
            sb.append(fl.toString());
            sb.append(" }");
            sb.append(System.getProperty("line.separator"));
        });
        sb.append("]");
        sb.append(System.getProperty("line.separator"));

        sb.append("Table Names : [");
        tableNames.forEach((tb, value) -> {
            sb.append(tb);
            sb.append(" : ");
            sb.append(value);
            sb.append(System.getProperty("line.separator"));
        });
        sb.append(System.getProperty("line.separator"));

        sb.append("Options : [");
        options.forEach((op, value) -> {
            sb.append(op);
            sb.append(" : ");
            sb.append(value.toString());
            sb.append(System.getProperty("line.separator"));
        });
        sb.append(System.getProperty("line.separator"));

        return sb.toString();
    }

    /**
     * Generate random rows based on this model
     *
     * @param number of rows to generate
     * @return list of rows
     */
    public List<Row> generateRandomRows(long number) {
        List<Row> rows = new ArrayList<>();

        for (long i = 0; i < number; i++) {
            Row row = new Row();
            // A linkedHashMap is required to keep order in fields (which should be the same than fields from Model)
            LinkedHashMap<T, Object> valuesMap = new LinkedHashMap<>();
            for(T f: fields) {
                valuesMap.put(f, f.generateRandomValue());
            }
            row.setValues(valuesMap);
            row.populatePksValues(primaryKeys);
            rows.add(row);
        }

        return rows;
    }

    private Map<OptionsConverter.PrimaryKeys, List<T>> convertPrimaryKeys(Map<String, List<String>> pks) {
        Map<OptionsConverter.PrimaryKeys, List<T>> pksConverted = new HashMap<>();
        pks.forEach((k, v) -> {
            OptionsConverter.PrimaryKeys pk = OptionsConverter.convertOptionToPrimaryKey(k);
            if (pk != null) {
                pksConverted.put(pk, v.stream().map(this::findFieldWhoseNameIs).collect(Collectors.toList()));
            }
        });
        return pksConverted;
    }

    private Map<OptionsConverter.TableNames, String> convertTableNames(Map<String, String> tbs) {
        Map<OptionsConverter.TableNames, String> tbsConverted = new HashMap<>();
        tbs.forEach((k, v) -> {
            OptionsConverter.TableNames tb = OptionsConverter.convertOptionToTableNames(k);
            if (tb != null) {
                tbsConverted.put(tb, v);
            }
        });
        return tbsConverted;
    }

    /**
     * Depending on the option passed, option have special treatment to generate special required Object associated with
     *
     * @param ops passed from parsed file (as a map of string -> string)
     * @return options using Options enum and their associated object in form of a map
     */
    private Map<OptionsConverter.Options, Object> convertOptions(Map<String, String> ops) {
        Map<OptionsConverter.Options, Object> optionsFormatted = new HashMap<>();
        ops.forEach((k, v) -> {
            OptionsConverter.Options op = OptionsConverter.convertOptionToOption(k);
            if (op != null) {
                if (op == OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING) {
                    optionsFormatted.put(op, convertHbaseColFamilyOption(v));
                } else if (op == OptionsConverter.Options.SOLR_REPLICAS || op == OptionsConverter.Options.SOLR_SHARDS) {
                    optionsFormatted.put(op, Integer.valueOf(v));
                }
            }
        });
        return optionsFormatted;
    }

    /**
     * Format for HBase column family should be :
     * columnQualifier:ListOfColsSeparatedByAcomma;columnQualifier:ListOfColsSeparatedByAcomma ...
     * Goal is to Convert this option into a map of field to hbase column qualifier
     *
     * @param ops
     * @return
     */
    Map<T, String> convertHbaseColFamilyOption(String ops) {
        Map<T, String> hbaseFamilyColsMap = new HashMap<>();
        for (String s : ops.split(";")) {
            String cq = s.split(":")[0];
            for (String c : s.split(":")[1].split(",")) {
                T field = findFieldWhoseNameIs(c);
                if (field != null) {
                    hbaseFamilyColsMap.put(field, cq);
                }
            }
        }
        return hbaseFamilyColsMap;
    }

    public Set<String> getHBaseColumnFamilyList() {
        Map<T, String> colFamiliesMap = (Map<T, String>) options.get(OptionsConverter.Options.HBASE_COLUMN_FAMILIES_MAPPING);
        return new HashSet<>(colFamiliesMap.values());
    }

    private T findFieldWhoseNameIs(String name) {
        T fieldToFind = null;
        for (T f : fields) {
            if (f.name.equalsIgnoreCase(name)) {
                fieldToFind = f;
            }
        }
        if (fieldToFind == null) {
            logger.warn("The field specified in options " + name + " has not been found !!! " +
                    "=> A review of JSON file should be made to ensure it is consistent");
        }
        return fieldToFind;
    }

    private void setupFieldHbaseColQualifier(Map<T, String> fieldHbaseColMap) {
        if(fieldHbaseColMap!=null && !fieldHbaseColMap.isEmpty()) {
            fieldHbaseColMap.forEach(Field::setHbaseColumnQualifier);
        }
    }


    public String getSQLSchema() {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        fields.forEach(f -> {
            sb.append(f.name);
            sb.append(" ");
            sb.append(f.getHiveType());
            sb.append(", ");
        });
        sb.deleteCharAt(sb.length() - 2);
        sb.append(") ");
        logger.debug("Schema is : " + sb.toString());
        return sb.toString();
    }

    public String getInsertSQLStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        fields.forEach(f -> {
            sb.append(f.name);
            sb.append(", ");
        });
        sb.deleteCharAt(sb.length() - 2);
        sb.append(") VALUES ( ");
        fields.forEach(f -> sb.append("?, "));
        sb.deleteCharAt(sb.length() - 2);
        sb.append(" ) ");
        logger.debug("Insert is : " + sb.toString());
        return sb.toString();
    }

    public String getCsvHeader() {
        StringBuilder sb = new StringBuilder();
        fields.forEach(f -> {
            sb.append(f.name);
            sb.append(",");
        });
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }




    // TODO: Implement verifications on the model before starting (not two same names of field, primary keys defined)
    // Ozone bucket and volume should be string between 3-63 characters (No upper case)
    // Kafka topic should not have special characters or "-"
    public void verifyModel() { }

}
