/**
 *
 */
package com.github.tlrx.elasticsearch.samples.search;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchBulkRequest;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchIndex;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchMapping;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchMappingField;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.annotations.Store;
import com.github.tlrx.elasticsearch.test.annotations.Types;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;

/**
 * Test Java API / Search : Filters
 *
 * @author tlrx
 */
@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
public class FilterTest {

    private static final String INDEX = "books";

    @ElasticsearchClient
    Client client;

    @Test
    @ElasticsearchIndex(indexName = INDEX,
            mappings = {@ElasticsearchMapping(typeName = "book",
                    properties = {
                            @ElasticsearchMappingField(name = "title", store = Store.Yes, type = Types.String),
                            @ElasticsearchMappingField(name = "tags", store = Store.Yes, type = Types.String),
                            @ElasticsearchMappingField(name = "year", store = Store.Yes, type = Types.Integer),
                            @ElasticsearchMappingField(name = "author.firstname", store = Store.Yes, type = Types.String),
                            @ElasticsearchMappingField(name = "author.lastname", store = Store.Yes, type = Types.String)
                    })
            })
    @ElasticsearchBulkRequest(dataFile = "com/github/tlrx/elasticsearch/samples/search/FilterTest.json")
    public void testFilters() throws IOException {

        // Verify if bulk import succeed
        SearchResponse response = client.prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertEquals(7L, response.getHits().totalHits());

        // Search for match_all and filter "tags:french"
        
        response = client.prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "kimchy")))
                .execute()
                .actionGet();
        assertEquals(7L, response.getHits().totalHits());

        // Search for match_all and filter "tags:poetry"
        response = client.prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("tags", "poetry")))
                .execute()
                .actionGet();
        assertEquals(3L, response.getHits().totalHits());

        // Search for match_all and filter "tags:literature" and "year:1829"
        response = client.prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("tags", "literature"))
                		.must(QueryBuilders.termQuery("year", "1829")))
                .execute()
                .actionGet();
        assertEquals(2L, response.getHits().totalHits());

    }
}
