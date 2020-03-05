package com.cloudera.frisch.randomdatagen;


import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Random;


public class Utils {

    private Utils() { throw new IllegalStateException("Could not initialize this class"); }

    private static final Logger logger = Logger.getLogger(Utils.class);

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


}
