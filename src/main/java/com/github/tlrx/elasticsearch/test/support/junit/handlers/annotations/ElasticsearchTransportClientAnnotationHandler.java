/**
 *
 */
package com.github.tlrx.elasticsearch.test.support.junit.handlers.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Logger;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchTransportClient;
import com.github.tlrx.elasticsearch.test.support.junit.handlers.ClassLevelElasticsearchAnnotationHandler;
import com.github.tlrx.elasticsearch.test.support.junit.handlers.FieldLevelElasticsearchAnnotationHandler;

/**
 * Handle {@link ElasticsearchTransportClient} annotation
 *
 * @author tlrx
 */
public class ElasticsearchTransportClientAnnotationHandler implements ClassLevelElasticsearchAnnotationHandler, FieldLevelElasticsearchAnnotationHandler {

    private final static Logger LOGGER = Logger.getLogger(ElasticsearchTransportClientAnnotationHandler.class.getName());

    public boolean support(Annotation annotation) {
        return (annotation instanceof ElasticsearchTransportClient);
    }

    public void handleField(Annotation annotation, Object instance, Map<String, Object> context, Field field) {
        ElasticsearchTransportClient elasticsearchTransportClient = (ElasticsearchTransportClient) annotation;

        // Settings
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", String.valueOf(elasticsearchTransportClient.clusterName()))
                .build();

        TransportClient client = TransportClient.builder().settings(settings).build();

        int n = 0;
        for (String host : elasticsearchTransportClient.hostnames()) {
            try {
	            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), elasticsearchTransportClient.ports()[n++]));
          
            } catch (UnknownHostException e) {
	            throw new RuntimeException("Not able to get InetAddress by host : "  + host);
            }
        }

        if (client != null) {
            try {
                field.setAccessible(true);
                field.set(instance, client);

                context.put(client.toString(), client);
            } catch (Exception e) {
                LOGGER.severe("Unable to set transport client for field " + field.getName() + ":" + e.getMessage());
            }
        }
    }

    public void beforeClass(Object testClass, Map<String, Object> context) throws Exception {
        // Nothing to do here
    }

    public void handleBeforeClass(Annotation annotation, Object testClass, Map<String, Object> context) throws Exception {
        // Nothing to do here
    }

    public void handleAfterClass(Annotation annotation, Object testClass, Map<String, Object> context) throws Exception {
        // Nothing to do here
    }

    public void afterClass(Object testClass, Map<String, Object> context) throws Exception {
        // Closing all TransportClient
        for (Object obj : context.values()) {
            if (obj instanceof TransportClient) {
                TransportClient client = (TransportClient) obj;
                client.close();
            }
        }
    }
}
