package com.yahoo.ycsb.db;

import com.google.common.collect.Multiset;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.index.query.FilterBuilders.*;


import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

/**
 * Created by chuka on 8/13/15.
 */
public class AElasticSearchClient extends DB {

    public static final String DEFAULT_CLUSTER_NAME = "aes.ycsb.cluster";
    public static final String DEFAULT_INDEX_KEY = "chookah.ycsb";
    public static final String DEFAULT_REMOTE_HOST = "localhost:9300";
    private static final String METHOD = "http://";

    private Boolean remoteMode;
    private String indexKey;
    private String clusterName;
    private JestClient client;

    /**
     * Initialize state for AElasticSearch. Called once per DB Instance
     * @throws DBException
     */
    @Override
    public void init() throws DBException {
        //Initialize our JEST drivers
        //Note: we do not allow dynamic creation of ES clusters like the ElasticsearchClient
        Properties properties = getProperties();
        this.indexKey = properties.getProperty("es.index.key", DEFAULT_INDEX_KEY);
        this.clusterName = properties.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
        String nodeList[] =  properties.getProperty("elasticsearch.hosts.list", DEFAULT_REMOTE_HOST).split(",");
        Boolean createNewDB = Boolean.parseBoolean(properties.getProperty("elasticsearch.newdb", "false"));
        System.out.println("ElasticSearch Remote Hosts = " +properties.getProperty("elasticsearch.hosts.list", DEFAULT_REMOTE_HOST));

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                                .Builder(METHOD + nodeList[0])
                                .multiThreaded(true)
                                .build());
        client = factory.getObject();

        if(createNewDB) {
            //Delete then create a new index
            try {
                client.execute(new DeleteIndex.Builder(this.indexKey).build());
                client.execute(new CreateIndex.Builder(this.indexKey).build());
            } catch (IOException ex) {
                System.err.println("Failed to initialize your database");
                ex.printStackTrace();
            }

        } else {
            //Only create a new index if nothing else exists (on first run)
            JestResult result;
            Action action = new IndicesExists.Builder(this.indexKey).build();
            try {
                result = client.execute(action);
                if (!result.isSucceeded())
                    client.execute(new CreateIndex.Builder(this.indexKey).build());
            } catch (IOException ex) {
                System.err.println("Failed to determine if an existing index was setup");
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void cleanup() throws DBException {
        client.shutdownClient();
    }

    /**
     * Insert a record into the database. Any field/value pairs in the specified
     * values will be written into the record with the specified record.
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            final XContentBuilder doc = jsonBuilder().startObject();
            for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
                doc.field(entry.getKey(), entry.getValue());
            }

            String source = doc.endObject().string();
            Index index = new Index.Builder(source).index(this.indexKey).type(table).id(key).build();
            client.execute(index);
            return 0;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return 1;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return
     */
    @Override
    public int delete(String table, String key) {
        try {
            client.execute(new Delete.Builder(key)
                                .index(this.indexKey)
                                .type(table)
                                .build());
            return 0;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return 1;
    }


    /**
     * Read a record from the database. Each field/value pair will be stored in a Hashmap
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            Get get = new Get.Builder(this.indexKey, key).type(table).build();
            final JestResult response = client.execute(get);

            if (fields != null) {
                for (String field : fields) {
                    result.put((String)field, new StringByteIterator((String) response.getValue(field)));
                }
            } else {
                for (Object field : response.getJsonMap().keySet()){
                    result.put((String)field, new StringByteIterator((String) response.getValue((String)field)));
                }
            }
            return 0;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return 1;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values
     * HashMap will be written into the record with the specified record key, overwriting
     * existing values with the same key.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Get get = new Get.Builder(this.indexKey, key).type(table).build();
            final JestResult response = client.execute(get);

            for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
                response.getJsonMap().put(entry.getKey(), entry.getValue());
            }
            String source = response.getJsonString();
            Index index = new Index.Builder(source).index(this.indexKey).type(table).id(key).build();
            client.execute(index);
            return 0;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return 1;
    }

    /**
     * Perform a range scan for a set of records in the database.
     * Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            final RangeFilterBuilder filter = rangeFilter("_id").gte(startkey);

            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.filter(filter);
            Search search = new Search.Builder(searchSourceBuilder.toString())
                                            .addIndex(this.indexKey)
                                            .build();
            SearchResult searchResult = client.execute(search);

            //TODO: iterate over results


        } catch(Exception ex) {

        }
        return 1;
    }
}
