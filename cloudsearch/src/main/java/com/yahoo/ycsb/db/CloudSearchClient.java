package com.yahoo.ycsb.db;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
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

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;

import java.nio.charset.Charset;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.HashMap;
import java.util.Date;
import java.util.Set;

/**
 *	CloudSearch client for YCSB
 * @author Chuka Okoye
 *
 */
public class CloudSearchClient extends DB {

    private static final String TIMEOUT = "10000";
    private static final String RETRY_COUNT = "0";
    private static final String API = "2013";
    private static final String DEFAULT_ACCESS_KEY = null;
    private static final String DEFAULT_SECRET_KEY = null;
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
            admin = new com.amazonaws.services.cloudsearch.AmazonCloudSearchClient(getCredentials(), config);
            this.batchURL = "http://" + docEndpoint + "/" + CLOUDSEARCH_2011_VERSION + "/documents/batch";
        }
        else {
            admin = new com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient(getCredentials(), config);
            this.batchURL = "http://" + docEndpoint + "/" + CLOUDSEARCH_2013_VERSION + "/documents/batch";
        }

        domainClient.setEndpoint(this.searchEndpoint);
        admin.setEndpoint(this.searchEndpoint);

        domainClient.setSignerRegionOverride(this.region);
        admin.setSignerRegionOverride(this.region);
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
        throw new UnsupportedOperationException();
    }

    /**
     * Read a record from CloudSearch. We only use the table name and key when
     * conducting search queries.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result){
        //All read should do is simple conduct a search and see if it is successful or not.
        //If successful code, return 0, otherwise return 1.

        //First, build a SearchRequest object
        SearchResult searchResult;
        SearchRequest searchRequest = new SearchRequest();
        ResponseMetadata response;
        boolean errors = false;

        try{
            String query = "mariecurie";
            searchRequest.setQuery(query);
            searchResult = ((AmazonCloudSearchDomainClient)client).search(searchRequest);
            if (searchResult.getHits().getFound() < 1){
                throw new Exception("Error! Unexpected no of hits found!");
            }
        }
        catch(AmazonServiceException ase){
            System.err.println("Experiencing problems with the service");
            errors = true;
            throw ase;
        }
        catch(Exception ex){
            System.err.println("An unknown error occured when searching/reading results from cloudsearch "+ex.toString());
            ex.printStackTrace();
            errors = true;
        }
        return errors == false ? 0 : 1;
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
        throw new UnsupportedOperationException();
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


    private void post(JSONArray batch) throws Exception {
        if (this.apiVersion == 2011)
            post2011(batch);
        else
            post2013(batch);
    }

    private void post2013(JSONArray batch) throws Exception {
        //pass
    }

    private JSONObject post2011(JSONArray batch) throws Exception {
        String sdf = batch.toString();
        StringEntity entity = new StringEntity(sdf, CONTENT_TYPE);
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(this.batchURL);
        httpPost.setEntity(entity);

        BasicResponseHandler responseHandler = new BasicResponseHandler();
        String responseString = httpClient.execute(httpPost, responseHandler);
        JSONObject response = new JSONObject(responseString);
        return response;
    }
}