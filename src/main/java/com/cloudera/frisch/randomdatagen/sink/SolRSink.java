package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

import java.util.List;
import java.util.stream.Collectors;


/**
 * This is a SolR sink using 8.4.0 API
 * Each instance is linked to a unique SolR collection
 */
public class SolRSink implements SinkInterface {

    private HttpSolrClient httpSolrClient;
    private String collection;


    public void init(Model model) {

        String protocol = "http";

        if(Boolean.parseBoolean(PropertiesLoader.getProperty("solr.security.ssl"))) {
            System.setProperty("javax.net.ssl.trustStore", PropertiesLoader.getProperty("solr.truststore.location"));
            System.setProperty("javax.net.ssl.trustStorePassword", PropertiesLoader.getProperty("solr.truststore.password"));

            protocol = "https";
        }

        HttpSolrClient.Builder solrClientBuilder = new HttpSolrClient.Builder(protocol + "://" +
                PropertiesLoader.getProperty("solr.server.url") + ":" +
                PropertiesLoader.getProperty("solr.server.port") + "/solr")
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000);

        if (Boolean.parseBoolean(PropertiesLoader.getProperty("solr.auth.kerberos"))) {
            Utils.createJaasConfigFile("solr-jaas-randomdatagen.config", "SolrJClient",
                    PropertiesLoader.getProperty("solr.auth.kerberos.keytab"),
                    PropertiesLoader.getProperty("solr.auth.kerberos.user"),
                    true, null, false);
            System.setProperty("java.security.auth.login.config", "solr-jaas-randomdatagen.config");
            System.setProperty("solr.kerberos.jaas.appname", "SolrJClient");

            try(Krb5HttpClientBuilder krb5HttpClientBuilder = new Krb5HttpClientBuilder()) {
                HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder.getHttpClientBuilder(java.util.Optional.empty()));
            } catch (Exception e) {
                logger.warn("Could set Kerberos for HTTP client due to error:", e);
            }

        }

        httpSolrClient = solrClientBuilder.build();

        collection = (String) model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION);
        createSolRCollectionIfNotExists(collection, model);

        // Set base URL directly to the collection, note that this is required
        httpSolrClient.setBaseURL(protocol + "://" + PropertiesLoader.getProperty("solr.server.url") + ":" +
                PropertiesLoader.getProperty("solr.server.port") + "/solr/" + collection);
    }

    public void terminate() {
        try {
            httpSolrClient.close();
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
            httpSolrClient.request(
                    CollectionAdminRequest.createCollection(collection,
                            (Integer) model.getOptions().get(OptionsConverter.Options.SOLR_SHARDS),
                            (Integer) model.getOptions().get(OptionsConverter.Options.SOLR_REPLICAS))
            );
            logger.debug("Finished to create collection : " + collection + " in SolR");
        } catch (BaseHttpSolrClient.RemoteSolrException e) {
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
