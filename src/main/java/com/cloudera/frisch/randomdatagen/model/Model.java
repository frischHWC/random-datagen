package com.cloudera.frisch.randomdatagen.model;


import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;

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
    @Getter @Setter
    private ConditionalEvaluator conditionalEvaluator;

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
        this.conditionalEvaluator = new ConditionalEvaluator(this);

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
            rows.add(conditionalEvaluator.evaluateConditions(row, this));
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
                } else if (op == OptionsConverter.Options.SOLR_REPLICAS || op == OptionsConverter.Options.SOLR_SHARDS
                    || op == OptionsConverter.Options.KUDU_REPLICAS || op == OptionsConverter.Options.HIVE_THREAD_NUMBER) {
                    optionsFormatted.put(op, Integer.valueOf(v));
                } else if (op == OptionsConverter.Options.LOCAL_FILE_ONE_PER_ITERATION || op == OptionsConverter.Options.HIVE_ON_HDFS) {
                    optionsFormatted.put(op, Boolean.valueOf(v));
                } else {
                    optionsFormatted.put(op, v);
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

    T findFieldWhoseNameIs(String name) {
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

    public Field findFieldasFieldWhoseNameIs(String name) {
        Field fieldToFind = null;
        for (Field f : fields) {
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

    public Schema getKuduSchema() {
        List<ColumnSchema> columns = new LinkedList<>();
        fields.forEach(f -> {
            boolean isaPK = false;
            for (String k : getKuduPrimaryKeys()) {
                if (k.equalsIgnoreCase(f.name)) {isaPK = true;}
            }
            if (isaPK) {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(f.name, f.getKuduType())
                    .key(true)
                    .build());
            } else {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(f.name, f.getKuduType())
                    .build());
            }
        });

        return new Schema(columns);
    }

    public List<String> getKuduPrimaryKeys() {
        List<T> kuduPrimaryKeys = primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_PRIMARY_KEYS);
        List<String> hashKeys = new ArrayList<>(kuduPrimaryKeys.size());
        kuduPrimaryKeys.forEach(f -> hashKeys.add(f.name));
        return hashKeys;
    }

    public List<String> getKuduRangeKeys() {
        List<T> kuduRangeKeys = primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_RANGE_KEYS);
        List<String> hashKeys = new ArrayList<>(kuduRangeKeys.size());
        kuduRangeKeys.forEach(f -> hashKeys.add(f.name));
        return hashKeys;
    }

    public List<String> getKuduHashKeys() {
        List<T> kuduHashKeys = primaryKeys.get(OptionsConverter.PrimaryKeys.KUDU_HASH_KEYS);
        List<String> hashKeys = new ArrayList<>(kuduHashKeys.size());
        kuduHashKeys.forEach(f -> hashKeys.add(f.name));
        return hashKeys;
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

    public org.apache.avro.Schema getAvroSchema() {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaBuilder = SchemaBuilder
                .record(tableNames.get(OptionsConverter.TableNames.AVRO_NAME))
                .namespace("org.apache.avro.ipc")
                .fields();

        for(T field: fields) {
            schemaBuilder = schemaBuilder.name(field.name).type(field.getGenericRecordType()).noDefault();
        }

        return schemaBuilder.endRecord();
    }

    public TypeDescription getOrcSchema() {
        TypeDescription typeDescription = TypeDescription.createStruct();
        fields.forEach(field -> typeDescription.addField(field.name, field.getTypeDescriptionOrc()));
        return typeDescription;
    }

    public Map<T, ColumnVector> createOrcVectors(VectorizedRowBatch batch) {
        LinkedHashMap<T, ColumnVector> hashMap = new LinkedHashMap<>();
        int cols = 0;
        for(T field: fields) {
            hashMap.put(field, field.getOrcColumnVector(batch, cols));
            cols++;
        }
        return hashMap;
    }

    // TODO: Implement verifications on the model before starting (not two same names of field, primary keys defined)
    // Ozone bucket and volume should be string between 3-63 characters (No upper case)
    // Kafka topic should not have special characters or "-"
    public void verifyModel() { }

}
