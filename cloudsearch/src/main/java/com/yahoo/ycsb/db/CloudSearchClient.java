package com.yahoo.ycsb.db;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.SearchRequest;
import com.amazonaws.services.cloudsearchdomain.model.SearchResult;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.JSONArray;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.http.StatusLine;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.URI;
import java.util.Map.Entry;

/**
 *	CloudSearch client for YCSB
 * @author Chuka Okoye
 *
 */
public class CloudSearchClient extends DB {

    private static final String TIMEOUT = "10000";
    private static final String RETRY_COUNT = "0";
    private static final String API = "2013";
    private static final String DEFAULT_ACCESS_KEY = "";
    private static final String DEFAULT_SECRET_KEY = "";
    private static final String REGION = "us-east-1";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/json", Charset.forName("UTF-8"));
    private static final String CLOUDSEARCH_2011_VERSION = "2011-02-01";
    private static final String CLOUDSEARCH_2013_VERSION = "2013-01-01";

    private AmazonCloudSearchDomainClient domainClient = null;
    private AmazonWebServiceClient admin = null;
    private Boolean debug;
    private String searchEndpoint;
    private String docEndpoint;
    private String accessKey;
    private String secretKey;
    private String region;
    private String batchURL;
    private int timeout;
    private int retryCount;
    private int apiVersion;

    @Override
    public void init() throws DBException {
        Properties properties = getProperties();
        this.debug = Boolean.parseBoolean(properties.getProperty("cloudsearch.debug", "false"));
        this.searchEndpoint = properties.getProperty("cloudsearch.search.endpoint");
        this.docEndpoint = properties.getProperty("cloudsearch.doc.endpoint");
        this.timeout = Integer.parseInt(properties.getProperty("cloudsearch.sockettimeout", TIMEOUT));
        this.retryCount = Integer.parseInt(properties.getProperty("cloudsearch.retrycount", RETRY_COUNT));
        this.apiVersion = Integer.parseInt(properties.getProperty("cloudsearch.api", API));
        this.accessKey = properties.getProperty("aws.accesskey", DEFAULT_ACCESS_KEY);
        this.secretKey = properties.getProperty("aws.secretkey", DEFAULT_SECRET_KEY);
        this.region = properties.getProperty("aws.region", REGION);



        ClientConfiguration config = new ClientConfiguration();
        config.setSocketTimeout(this.timeout);
        config.setMaxErrorRetry(this.retryCount);
        config.setRetryPolicy(new RetryPolicy(null, null, this.retryCount, true));

        if (haveCredentials())
            domainClient = new AmazonCloudSearchDomainClient(getCredentials(), config);
        else
            domainClient = new AmazonCloudSearchDomainClient(config);

        if (apiVersion == 2011){
            if (haveCredentials())
                admin = new com.amazonaws.services.cloudsearch.AmazonCloudSearchClient(getCredentials(), config);
            else
                admin = new com.amazonaws.services.cloudsearch.AmazonCloudSearchClient(config);
            this.batchURL = "http://" + docEndpoint + "/" + CLOUDSEARCH_2011_VERSION + "/documents/batch";
        }
        else {
            if (haveCredentials())
                admin = new com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient(getCredentials(), config);
            else
                admin = new com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient(config);
            this.batchURL = "http://" + docEndpoint + "/" + CLOUDSEARCH_2013_VERSION + "/documents/batch";
        }

        domainClient.setEndpoint(this.searchEndpoint);
        admin.setEndpoint(this.searchEndpoint);

        domainClient.setSignerRegionOverride(this.region);
        admin.setSignerRegionOverride(this.region);

        if (debug){
            System.err.println("Succesfully initialized CloudSearch driver state for Thread "+Thread.currentThread().getId());
        }
    }

    @Override
    public void cleanup() throws DBException {
        domainClient.shutdown();
        admin.shutdown();
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values){
        JSONObject doc = new JSONObject();
        JSONObject fields = new JSONObject();
        JSONArray batch = new JSONArray();
        JSONObject response;
        try{
            for(Entry<String, String>entry : StringByteIterator.getStringMap(values).entrySet()){
                fields.accumulate(entry.getKey(), entry.getValue());
            }
            fields.accumulate("table", table); //Simulation of a table
            doc.put("type", "add");
            doc.put("id", key);
            doc.put("lang", "en"); //not really english but should not matter.
            doc.put("version", (int)(new Date().getTime() / 1000));
            doc.put("fields", fields);
            batch.put(doc);
            post(batch);
        }
        catch (Exception ex){
            ex.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * Delete a record from the CloudSearch Domain.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        JSONArray batch = new JSONArray();
        JSONObject doc = new JSONObject();
        try{
            doc.put("type", "delete");
            doc.put("id", key);
            doc.put("version", (int)(new Date().getTime() / 1000));
            batch.put(doc);
            post(batch);
        }
        catch(Exception ex){
            ex.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * Update a record in the CloudSearch domain. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return this.insert(table, key, values);
    }

    /**
     * RangeOperations are currently unsupported and have no real equivalent for searching.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            String response = search(table);
            JSONObject jsonResponse = new JSONObject(response);
            //TODO: We need to iterate over results.
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * Read a record from the database. Each field/value pair will be stored in a Hashmap
     *
     * @param table The name of the table
     * @param key The record key of the record to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result){
        try {
            String response = search(key);
            System.err.println(response);
            //JSONObject jsonResponse = new JSONObject(response);
            // TODO: Need to assert only 1 record is returned.
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * Are the necessary credentials available in our config file
     * if yes, instantiate the credentials object.
     * @return true if credentials were supplied in config file
     */
    private boolean haveCredentials(){
        return this.accessKey != null && this.secretKey != null;
    }

    private AWSCredentials getCredentials(){
        return new BasicAWSCredentials(this.accessKey, this.secretKey);
    }

    private JSONObject post(JSONArray batch) throws Exception {
        String sdf = batch.toString();
        StringEntity entity = new StringEntity(sdf, CONTENT_TYPE);
        HttpClient httpClient = HttpClientBuilder.create().disableAutomaticRetries().build();
        HttpPost httpPost = new HttpPost(this.batchURL);
        httpPost.setEntity(entity);

        BasicResponseHandler responseHandler = new BasicResponseHandler();
        String responseString = httpClient.execute(httpPost, responseHandler);
        JSONObject response = new JSONObject(responseString);
        return response;
    }

    private String search(String query) throws Exception {
        if (this.apiVersion == 2011)
            return search2011(query);
        else if (this.apiVersion == 2013)
            return search2013(query);
        else
            throw new Exception("Unsupported CloudSearch version specified");
    }

    private String search2011(String query) throws Exception {
        URIBuilder builder = new URIBuilder();
        URI uri;
        HttpRequestBase httpRequest;
        HttpClient httpClient;
        HttpResponse response;
        StatusLine statusLine;

        builder.addParameter("q", query);
        uri = builder.setScheme("http").setHost(this.searchEndpoint).build();
        httpRequest = new HttpGet(uri);

        if (debug){
            System.err.println("Fetching:"
                + URLDecoder.decode(uri.toASCIIString(),
                    StandardCharsets.UTF_8.toString()));
        }
        httpClient = HttpClientBuilder.create().disableAutomaticRetries().build();
        response = httpClient.execute(httpRequest);
        statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() != 200){
            throw new Exception("Search request failed with code " + statusLine.getStatusCode() + " and URI "+uri);
        }
        return EntityUtils.toString(response.getEntity());
    }

    private String search2013(String query) throws Exception {
        SearchResult searchResult;
        SearchRequest searchRequest = new SearchRequest();

        searchRequest.setQuery(query);
        searchResult = domainClient.search(searchRequest);
        if (searchResult.getHits().getFound() < 1)
            throw new Exception("Error, no documents were retrieved in query");

        return searchResult.toString();
    }
}