package com.cloudera.frisch.randomdatagen;


import com.cloudera.frisch.randomdatagen.config.ArgumentsParser;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.sink.*;
import org.apache.avro.reflect.MapEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class Utils {

    private Utils() { throw new IllegalStateException("Could not initialize this class"); }

    private static final Logger logger = Logger.getLogger(Utils.class);

    private static final long oneHour = 1000 * 60 *60;
    private static final long oneMinute = 1000 * 60;

    /**
     * Generates a random password between 5 & 35 characters composed of all possible characters
     * @param random
     * @return
     */
    public static String generateRandomPassword(Random random) {
        byte[] bytesArray = new byte[Math.abs(random.nextInt()%30) + 5];
        random.nextBytes(bytesArray);
        return new String(bytesArray, StandardCharsets.UTF_8);
    }

    /**
     * Generates a random AlphaNumeric string of specified length
     * @param n equals length of the string to generate
     * @param random Random object used to generate random string
     * @return
     */
    public static String getAlphaNumericString(int n, Random random)
    {
        // chose a Character random from this String
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";
        // create StringBuffer size of alphaNumericString
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to alphaNumericString variable length
            int index
                    = (int) (alphaNumericString.length()
                    * random.nextDouble());
            // add Character one by one in end of sb
            sb.append(alphaNumericString
                    .charAt(index));
        }
        return sb.toString();
    }

    /**
     * Generates a random Alpha string [A-Z] of specified length
     * @param n equals length of the string to generate
     * @param random Random object used to generate random string
     * @return
     */
    public static String getAlphaString(int n, Random random)
    {
        // chose a Character random from this String
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "abcdefghijklmnopqrstuvxyz";
        // create StringBuffer size of alphaNumericString
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to alphaNumericString variable length
            int index
                    = (int)(alphaNumericString.length()
                    * random.nextDouble());
            // add Character one by one in end of sb
            sb.append(alphaNumericString
                    .charAt(index));
        }
        return sb.toString();
    }


    /**
     * Using map of possible values weighted (between 0 and 100), it gives possible value
     * @param random
     * @param weights
     * @return
     */
    public static String getRandomValueWithWeights(Random random, LinkedHashMap<String, Integer> weights) {
        int randomIntPercentage = random.nextInt(100);
        int sumOfWeight = 0;
        for(Map.Entry<String, Integer> entry : weights.entrySet()) {
            sumOfWeight = sumOfWeight + entry.getValue();
            if(randomIntPercentage < sumOfWeight) {
                return entry.getKey();
            }
        }
        return "";
    }

    /**
     * Login to kerberos using a given user and its associated keytab
     * @param kerberosUser is the kerberos user
     * @param pathToKeytab path to the keytab associated with the user, note that unix read-right are needed to access it
     * @param config hadoop configuration used further
     */
    public static void loginUserWithKerberos(String kerberosUser, String pathToKeytab, Configuration config) {
        if(config != null) {
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
        }
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosUser, pathToKeytab);
            } catch (IOException e) {
                logger.error("Could not load keytab file",e);
            }

    }

    /**
     * Setup haddop env by setting up needed Hadoop system property and adding to configuration required files
     * @param config Hadoop configuration to set up
     */
    public static void setupHadoopEnv(Configuration config) {

        config.addResource(new Path("file://"+ PropertiesLoader.getProperty("hadoop.core.site.path")));
        config.addResource(new Path("file://"+ PropertiesLoader.getProperty("hadoop.hdfs.site.path")));
        config.addResource(new Path("file://"+ PropertiesLoader.getProperty("hadoop.ozone.site.path")));
        config.addResource(new Path("file://"+ PropertiesLoader.getProperty("hadoop.hbase.site.path")));

        System.setProperty("HADOOP_USER_NAME", PropertiesLoader.getProperty("hadoop.user"));
        System.setProperty("hadoop.home.dir", PropertiesLoader.getProperty("hadoop.home"));
    }

    /**
     * Delete all local files in a specified directory with a specified extension and a name
     * @param directory
     * @param extension
     */
    public static void deleteAllLocalFiles(String directory, String name, String extension) {
        File folder = new File(directory);
        File[] files = folder.listFiles((dir,f) -> f.matches(name + ".*[.]" + extension));
        for(File f: files){
            logger.debug("Will delete local file: " + f);
            if(!f.delete()) { logger.warn("Could not delete file: " + f);}
        }
    }

    /**
     * Delete all HDFS files in a specified directory with a specified extension and a name
     * @param directory
     * @param extension
     */
    public static void deleteAllHdfsFiles(FileSystem fileSystem, String directory, String name, String extension) {
        try {
            RemoteIterator<LocatedFileStatus> fileiterator =  fileSystem.listFiles(new Path(directory), false);
            while(fileiterator.hasNext()) {
                LocatedFileStatus file = fileiterator.next();
                if(file.getPath().getName().matches(name + ".*[.]" + extension)) {
                    logger.debug("Will delete HDFS file: " + file.getPath());
                    fileSystem.delete(file.getPath(), false);
                }
            }
        } catch (Exception e) {
            logger.warn("Could not delete files under " + directory + " due to error: ", e);
        }
    }

    /**
     * Creates a directory on HDFS
     * @param fileSystem
     * @param path
     */
    public static void createHdfsDirectory(FileSystem fileSystem, String path) {
        try {
            fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            logger.error("Unable to create hdfs directory of : " + path + " due to error: ", e);
        }
    }

    /**
     * Creates a directory locally
     * @param path
     */
    public static void createLocalDirectory(String path) {
        try {
            new File(path).mkdirs();
        } catch (Exception e) {
            logger.error("Unable to create directory of : " + path + " due to error: ", e);
        }
    }

    /**
     * Delete a file locally
     * @param path
     */
    public static void deleteLocalFile(String path) {
        try {
            new File(path).delete();
            logger.debug("Successfully delete local file : " + path);
        } catch (Exception e) {
            logger.error("Tried to delete file : " + path + " with no success :", e);
        }
    }

    /**
     * Delete an HDFS file
     * @param fileSystem
     * @param path
     */
    public static void deleteHdfsFile(FileSystem fileSystem, String path) {
        try {
            fileSystem.delete(new Path(path), true);
            logger.debug("Successfully deleted hdfs file : " + path);
        } catch (IOException e) {
            logger.error("Tried to delete hdfs file : " + path + " with no success :", e);
        }
    }


    /**
     * Write an JAAS config file that will be used by the application
     * Note that it overrides any existing files and its content
     * @param fileName File path + nam of jaas config file that will be created
     * @param clientName that will represent the client in the JAAS config file
     * @param keytabPath and name of the keytab to put on the file
     * @param principal in the form of principal@REALM as a string
     * @param useKeytab true/false or null if must not be set in the JAAS file
     * @param storeKey true/false or null if must not be set in the JAAS file
     */
    public static void createJaasConfigFile(String fileName, String clientName, String keytabPath, String principal, Boolean useKeytab, Boolean storeKey, Boolean appendToFile) {
        try(Writer fileWriter = new FileWriter(fileName, appendToFile)) {
            if(Boolean.TRUE.equals(appendToFile)) { fileWriter.append(System.getProperty("line.separator")); }
            fileWriter.append(clientName);
            fileWriter.append(" { ");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("com.sun.security.auth.module.Krb5LoginModule required");
            fileWriter.append(System.getProperty("line.separator"));
            if(useKeytab!=null) {
                fileWriter.append("useKeyTab=");
                fileWriter.append(useKeytab.toString());
                fileWriter.append(System.getProperty("line.separator"));
            }
            if(storeKey!=null) {
                fileWriter.append("storeKey=");
                fileWriter.append(storeKey.toString());
                fileWriter.append(System.getProperty("line.separator"));
            }
            fileWriter.append("keyTab=\"");
            fileWriter.append(keytabPath);
            fileWriter.append("\"");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("principal=\"");
            fileWriter.append(principal);
            fileWriter.append("\";");
            fileWriter.append(System.getProperty("line.separator"));
            fileWriter.append("};");
            fileWriter.flush();
        } catch (IOException e) {
            logger.error("Could not write proper JAAS config file : " + fileName + " due to error : ", e);
        }
    }

    /**
     * Given a time in milliseconds, format it to a better human comprehensive way
     * @param timeTaken
     * @return
     */
    public static String formatTimetaken(long timeTaken) {
        long timeTakenHere = timeTaken;
        String formattedTime = "";

        if(timeTakenHere >= oneHour ) {
            formattedTime = (timeTakenHere/oneHour) + "h ";
            timeTakenHere = timeTakenHere%oneHour;
        }

        if(timeTakenHere >= oneMinute) {
            formattedTime += (timeTakenHere/oneMinute) + "m ";
            timeTakenHere = timeTakenHere%oneMinute;
        }

        if(timeTakenHere > 1000) {
            formattedTime += (timeTakenHere / 1000) + "s ";
            timeTakenHere = timeTakenHere%1000;
        }

        formattedTime += timeTakenHere + "ms";
        
        return formattedTime;
    }

    /**
     * Test on classpath
     * Run it with /opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/bin/hadoop is different than /opt/cloudera/parcels/CDH/bin/hadoop
     */
    public static void testClasspath() {
        String classpath = System.getProperty("java.class.path");
        String[] paths = classpath.split(System.getProperty("path.separator"));
        logger.info("Home is : " + System.getProperty("java.home"));

        try(FileWriter myWriter = new FileWriter("/home/frisch/classpath.txt")) {
            for(String p: paths) {
                myWriter.write(p + System.getProperty("line.separator"));
                logger.info("This file is in path : " + p);
            }
            myWriter.flush();
        } catch (IOException e) {
            logger.error("Could not write to file", e);
        }
    }

    /**
     * Log in the recap of what's been generated
     */
    public static void recap(long numberOfBatches, long rowPerBatch, List<ArgumentsParser.sinks> sinks, Model model) {
        logger.info(" ************************* Recap of data generation ****************** ");
        logger.info("Generated " + rowPerBatch*numberOfBatches + " rows into : ");

        sinks.forEach(sink -> {
            switch (sink) {
            case HDFSCSV:
                logger.info("   - HDFS as CSV files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-0000000000.csv");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".csv");
                break;
            case HDFSJSON:
                logger.info("   - HDFS as JSON files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-0000000000.json");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".json");
                break;
            case HDFSAVRO:
                logger.info("   - HDFS as Avro files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-0000000000.avro");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".avro");
                break;
            case HDFSORC:
                logger.info("   - HDFS as ORC files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-0000000000.orc");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".orc");
                break;
            case HDFSPARQUET:
                logger.info("   - HDFS as Parquet files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-0000000000.parquet");
                logger.info("        to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.HDFS_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".parquet");
                break;
            case HBASE:
                logger.info("   - HBase in namespace " + model.getTableNames().get(OptionsConverter.TableNames.HBASE_NAMESPACE) +
                    " in table : " + model.getTableNames().get(OptionsConverter.TableNames.HBASE_TABLE_NAME));
                break;
            case HIVE:
                logger.info("   - Hive in database: " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE) +
                    " in table : " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME));
                if((Boolean) model.getOptions().get(OptionsConverter.Options.HIVE_ON_HDFS)) {
                    String tableNameTemporary = model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME)==null ?
                        (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TABLE_NAME) + "_tmp" :
                        (String) model.getTableNames().get(OptionsConverter.TableNames.HIVE_TEMPORARY_TABLE_NAME);
                    logger.info("   - Hive in database: " + model.getTableNames().get(OptionsConverter.TableNames.HIVE_DATABASE) +
                        " in external table : " + tableNameTemporary + " located in HDFS at: " +
                        model.getTableNames().get(OptionsConverter.TableNames.HIVE_HDFS_FILE_PATH));
                }
                break;
            case OZONE:
                logger.info("   - Ozone in volume " + model.getTableNames().get(OptionsConverter.TableNames.OZONE_VOLUME));
                break;
            case SOLR:
                logger.info("   - SolR in collection " + model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION));
                break;
            case KAFKA:
                logger.info("   - Kafka in topic " + model.getTableNames().get(OptionsConverter.TableNames.KAFKA_TOPIC));
                break;
            case KUDU:
                logger.info("   - Kudu in table " + model.getTableNames().get(OptionsConverter.TableNames.KUDU_TABLE_NAME));
                break;
            case CSV:
                logger.info("   - CSV files of " + rowPerBatch + " rows, from : " );
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.csv");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".csv");
                break;
            case JSON:
                logger.info("   - JSON files of " + rowPerBatch + " rows, from : " );
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.json");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".json");
                break;
            case AVRO:
                logger.info("   - Avro files of " + rowPerBatch + " rows, from : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.avro");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".avro");
                break;
            case PARQUET:
                logger.info("   - Parquet files of " + rowPerBatch + " rows, from : " );
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.parquet");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".parquet");
                break;
            case ORC:
                logger.info("   - ORC files of " + rowPerBatch + " rows, from : " );
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-0000000000.orc");
                logger.info("       to : ");
                logger.info("       " +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_PATH) +
                    model.getTableNames().get(OptionsConverter.TableNames.LOCAL_FILE_NAME) + "-" +
                    String.format("%010d", numberOfBatches-1) + ".orc");
                break;
            default:
                logger.info("The sink " + sink.toString() +
                    " provided has not been recognized as an expected sink");
                break;
            }

        });
        logger.info("****************************************************************");
    }


}
