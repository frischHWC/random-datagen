package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;

import java.util.List;
import java.util.stream.Collectors;


/**
 * This is a SolR sink using 8.4.0 API
 * Each instance is linked to a unique SolR collection
 */
public class SolRSink implements SinkInterface {

    private SolrServer httpSolrClient;
    private String collection;


    public void init(Model model) {

        String protocol = "http";

        if(Boolean.parseBoolean(PropertiesLoader.getProperty("solr.security.ssl"))) {
            System.setProperty("javax.net.ssl.trustStore", PropertiesLoader.getProperty("solr.truststore.location"));
            System.setProperty("javax.net.ssl.trustStorePassword", PropertiesLoader.getProperty("solr.truststore.password"));

            protocol = "https";
        }


        if (Boolean.parseBoolean(PropertiesLoader.getProperty("solr.auth.kerberos"))) {
            Utils.createJaasConfigFile("solr-jaas-randomdatagen.config", "SolrJClient",
                    PropertiesLoader.getProperty("solr.auth.kerberos.keytab"),
                    PropertiesLoader.getProperty("solr.auth.kerberos.user"),
                    true, null, false);
            System.setProperty("java.security.auth.login.config", "solr-jaas-randomdatagen.config");
            System.setProperty("solr.kerberos.jaas.appname", "SolrJClient");

            HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());

        }

        httpSolrClient =  new HttpSolrServer(protocol + "://" +
            PropertiesLoader.getProperty("solr.server.url") + ":" +
            PropertiesLoader.getProperty("solr.server.port") + "/solr");

        collection = (String) model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION);
        createSolRCollectionIfNotExists(collection, model);

        httpSolrClient =  new HttpSolrServer(protocol + "://" +
            PropertiesLoader.getProperty("solr.server.url") + ":" +
            PropertiesLoader.getProperty("solr.server.port") + "/solr/" + collection);

    }

    public void terminate() {
        try {
            httpSolrClient.shutdown();
        } catch (Exception e) {
            logger.error("Could not close connection to SolR due to error: ", e);
        }
    }

    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            httpSolrClient.add(
                    rows.parallelStream().map(Row::toSolRDoc).collect(Collectors.toList())
            );
            httpSolrClient.commit();
        } catch (Exception e) {
            logger.error("An unexpected error occurred while adding documents to SolR collection : " +
                    collection + " due to error:", e);
        }
    }

    private void createSolRCollectionIfNotExists(String collection, Model model) {
        try {
            logger.debug("Creating collection : " + collection + " in SolR");
            CollectionAdminRequest.createCollection(collection,
                (Integer) model.getOptions().get(OptionsConverter.Options.SOLR_SHARDS), "schemalessTemplate", httpSolrClient);
            CoreAdminRequest.createCore(collection, collection, httpSolrClient);

            logger.debug("Finished to create collection : " + collection + " in SolR");
        } catch (HttpSolrServer.RemoteSolrException e) {
            if (e.getMessage().contains("collection already exists")) {
                logger.warn("Collection already exists so it has not been created");
            } else {
                logger.error("Could not create SolR collection : " + collection + " due to error: ", e);
            }
        } catch (Exception e) {
            logger.error("Could not create SolR collection : " + collection + " due to error: ", e);
        }
    }
}
