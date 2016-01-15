/**
 *
 */
package com.github.tlrx.elasticsearch.test.support.junit.handlers.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchSetting;
import com.github.tlrx.elasticsearch.test.support.junit.handlers.ClassLevelElasticsearchAnnotationHandler;
import com.github.tlrx.elasticsearch.test.support.junit.handlers.FieldLevelElasticsearchAnnotationHandler;

/**
 * Handle {@link ElasticsearchNode} annotation
 *
 * @author tlrx
 */
public class ElasticsearchNodeAnnotationHandler implements ClassLevelElasticsearchAnnotationHandler, FieldLevelElasticsearchAnnotationHandler {

    /**
     * Elasticsearch home directory
     */
    private static final String ES_HOME = "./target/elasticsearch-test";
    private static final String NODE_NAME = "node.name";

    public boolean support(Annotation annotation) {
        return (annotation instanceof ElasticsearchNode);
    }

    public void beforeClass(Object testClass, Map<String, Object> context) throws Exception {
        // Nothing to do here
    }

    public void handleBeforeClass(Annotation annotation, Object testClass, Map<String, Object> context) {
        // Instantiate a node
        buildNode((ElasticsearchNode) annotation, context);
    }

    public void handleAfterClass(Annotation annotation, Object testClass, Map<String, Object> context) {
        // Nothing to do here
    }

    public void afterClass(Object testClass, Map<String, Object> context) throws Exception {
        for (Object obj : context.values()) {
            if (obj instanceof Node) {
                Node node = (Node) obj;

                if (!node.isClosed()) {
                    node.close();
                }
            }
        }
        Path path = Paths.get(ES_HOME);
        FileSystemUtils.deleteSubDirectories(path);
        IOUtils.rm(path);
    }

    public void handleField(Annotation annotation, Object instance, Map<String, Object> context, Field field) throws Exception {
        // Get the node
        Node node = buildNode((ElasticsearchNode) annotation, context);

        // Sets the node as the field's value
        try {
            field.setAccessible(true);
            field.set(instance, node);
        } catch (Exception e) {
            throw new Exception("Exception when setting the node:" + e.getMessage(), e);
        }
    }

    /**
     * Builds & start a new node, or retrieves an existing one from context
     *
     * @param elasticsearchNode
     * @param context
     * @return a {@link Node}
     */
    private Node buildNode(ElasticsearchNode elasticsearchNode, Map<String, Object> context) {

        // Create the node's settings
        Settings settings = buildNodeSettings(elasticsearchNode);

        // Search for the node in current context
        String nodeName = settings.get(NODE_NAME);
        Node node = (Node) context.get(nodeName);

        if (node == null) {
            // No node with this name has been found, let's instantiate a new one
            node = NodeBuilder.nodeBuilder()
                    .settings(settings)
                    .local(elasticsearchNode.local())
                    .node();
            context.put(nodeName, node);
        }
        return node;
    }

    /**
     * Build node settings
     */
    private Settings buildNodeSettings(ElasticsearchNode elasticsearchNode) {

        // Build default settings
        Builder settingsBuilder = Settings.settingsBuilder()
                .put(NODE_NAME, elasticsearchNode.name())
                .put("node.data", elasticsearchNode.data())
                .put("cluster.name", elasticsearchNode.clusterName())
                .put("index.store.type", "memory")
                .put("index.store.fs.memory.enabled", "true")
                .put("gateway.type", "none")
                .put("path.data", ES_HOME + "/data")
                .put("path.work", ES_HOME + "/work")
                .put("path.logs", ES_HOME + "/logs")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0");

        // Loads settings from configuration file
        Path path = Paths.get(elasticsearchNode.configFile());
        Settings configSettings = Settings.settingsBuilder().loadFromStream(path.getFileName().toString(), 
        		getClass().getClassLoader().getResourceAsStream(elasticsearchNode.configFile())).build();
        settingsBuilder.put(configSettings);

        // Other settings
        ElasticsearchSetting[] settings = elasticsearchNode.settings();
        for (ElasticsearchSetting setting : settings) {
            settingsBuilder.put(setting.name(), setting.value());
        }

        // Build the settings
        return settingsBuilder.build();
    }
}
