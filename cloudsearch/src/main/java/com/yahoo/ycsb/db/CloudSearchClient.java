package com.yahoo.ycsb.db;


import java.util.Set;
import java.util.Properties;


import org.json.simple.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.regions.Regions;
import com.amazonaws.regions.Region;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearch;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.SearchResult;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.SearchRequest;

/**
 *	CloudSearch client for YCSB
 * @author Chuka Okoye
 *
 */
public class CloudSearchClient extends DB {

    private AmazonCloudSearchDomainClient client = null; //Cloudsearch client
    private AWSCredentials credentials = null;

    /**
     * Initialize any state for this CloudSearch client instance.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        Properties properties = getProperties();
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        if (cloudSearchConfig.getSocketTimeout() >= 0) //use defaults otherwise
            clientConfiguration.setSocketTimeout(cloudSearchConfig.getSocketTimeout());
        if (cloudSearchConfig.getRetryCount() >= 0){ //use defaults otherwise
            clientConfiguration.setMaxErrorRetry(cloudSearchConfig.getRetryCount());
            clientConfiguration.setRetryPolicy(new RetryPolicy(null, null, cloudSearchConfig.getRetryCount(), true));
        }

        if (haveCredentials()){ //otherwise assume creds in env vars
            client = new AmazonCloudSearchDomainClient(credentials, clientConfiguration);
        }
        else{
            client = new AmazonCloudSearchDomainClient(clientConfiguration);
        }
        client.setEndpoint(cloudSearchConfig.getSearchEndpoint());
        client.setSignerRegionOverride(cloudSearchConfig.getRegion());
    }

    /**
     * Are the necessary credentials available in our config file
     * if yes, instantiate the credentials object.
     * @return true if credentials were supplied in config file
     */
    private boolean haveCredentials(){
        if (cloudSearchConfig.getAccessKeyId() != null && cloudSearchConfig.getSecretKeyId() != null){
            credentials = new BasicAWSCredentials(cloudSearchConfig.getAccessKeyId(), cloudSearchConfig.getSecretKeyId());
            return true;
        }
        return false;
    }

    @Override
    public void cleanup() throws DBException {
        client.shutdown();
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

        // TODO: implement this.
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
}