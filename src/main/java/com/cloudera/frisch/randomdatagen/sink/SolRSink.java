package com.cloudera.frisch.randomdatagen.sink;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.OptionsConverter;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This is a SolR sink using 8.4.0 API
 * Each instance is linked to a unique SolR collection
 */
@Slf4j
public class SolRSink implements SinkInterface {

    private final HttpSolrClient httpSolrClient;
    private final String collection;
    private final Model model;


    SolRSink(Model model, Map<ApplicationConfigs, String> properties) {
        this.collection = (String) model.getTableNames().get(OptionsConverter.TableNames.SOLR_COLLECTION);
        this.model = model;

        String protocol = "http";

        if(Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_TLS_ENABLED))) {
            System.setProperty("javax.net.ssl.keyStore", properties.get(ApplicationConfigs.SOLR_KEYSTORE_LOCATION));
            System.setProperty("javax.net.ssl.keyStorePassword", properties.get(ApplicationConfigs.SOLR_KEYSTORE_PASSWORD));
            System.setProperty("javax.net.ssl.trustStore", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION));
            System.setProperty("javax.net.ssl.trustStorePassword", properties.get(ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD));

            protocol = "https";
        }

        if (Boolean.parseBoolean(properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS))) {
            String jaasFilePath = (String) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_JAAS_FILE_PATH);
            Utils.createJaasConfigFile(jaasFilePath, "SolrJClient",
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB),
                    properties.get(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER),
                    true, true, false);
            System.setProperty("java.security.auth.login.config", jaasFilePath);
            System.setProperty("solr.kerberos.jaas.appname", "SolrJClient");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

            try(Krb5HttpClientBuilder krb5HttpClientBuilder = new Krb5HttpClientBuilder()) {
                HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder.getBuilder());
            } catch (Exception e) {
                log.warn("Could not set Kerberos for HTTP client due to error:", e);
            }
        }

        HttpSolrClient.Builder solrClientBuilder = new HttpSolrClient.Builder(protocol + "://" +
            properties.get(ApplicationConfigs.SOLR_SERVER_HOST).trim() + ":" +
            properties.get(ApplicationConfigs.SOLR_SERVER_PORT).trim() + "/solr")
            .withConnectionTimeout(10000)
            .withSocketTimeout(60000);

        this.httpSolrClient = solrClientBuilder.build();

        if ((Boolean) model.getOptionsOrDefault(OptionsConverter.Options.DELETE_PREVIOUS)) {
            deleteSolrCollection();
        }

        createSolRCollectionIfNotExists();

        // Set base URL directly to the collection, note that this is required
        httpSolrClient.setBaseURL(protocol + "://" + properties.get(ApplicationConfigs.SOLR_SERVER_HOST) + ":" +
            properties.get(ApplicationConfigs.SOLR_SERVER_PORT) + "/solr/" + collection);
    }

    @Override
    public void terminate() {
        try {
            httpSolrClient.close();
        } catch (Exception e) {
            log.error("Could not close connection to SolR due to error: ", e);
        }
    }

    @Override
    public void sendOneBatchOfRows(List<Row> rows) {
        try {
            httpSolrClient.add(
                    rows.parallelStream().map(Row::toSolRDoc).collect(Collectors.toList())
            );
            httpSolrClient.commit();
        } catch (Exception e) {
            log.error("An unexpected error occurred while adding documents to SolR collection : " +
                    collection + " due to error:", e);
        }
    }

    private void createSolRCollectionIfNotExists() {
        try {
            log.debug("Creating collection : " + collection + " in SolR");
            httpSolrClient.request(
                    CollectionAdminRequest.createCollection(collection,
                            (Integer) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_SHARDS),
                            (Integer) model.getOptionsOrDefault(OptionsConverter.Options.SOLR_REPLICAS))
            );
            log.debug("Finished to create collection : " + collection + " in SolR");
        } catch (BaseHttpSolrClient.RemoteSolrException e) {
            if (e.getMessage().contains("collection already exists")) {
                log.warn("Collection already exists so it has not been created");
            } else {
                log.error("Could not create SolR collection : " + collection + " due to error: ", e);
            }
        } catch (Exception e) {
            log.error("Could not create SolR collection : " + collection + " due to error: ", e);
        }
    }

    private void deleteSolrCollection() {
        try {
            httpSolrClient.request(CollectionAdminRequest.deleteCollection(collection));
        } catch (SolrServerException| IOException e) {
            log.error("Could not delete previous collection: " + collection + " due to error: ", e);
        }
    }
}
