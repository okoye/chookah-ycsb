package com.yahoo.ycsb.db;

import com.yahoo.ycsb.config.PropertiesConfig;
import java.util.Properties;

public class CloudSearchConfig extends PropertiesConfig{

    public static final String SEARCH_ENDPOINT = "cloudsearch.searchEndpoint";
    public static final String DOC_ENDPOINT = "cloudsearch.docEndpoint";
    public static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    public static final String AWS_SECRET_ACCESS_KEY = "aws.secretKey";
    public static final String DEBUG = "cloudsearch.debug";
    public static final String TIMEOUT = "cloudsearch.socketTimeout";
    public static final String MAX_RETRY_COUNT = "cloudsearch.retryCount";
    public static final String REGION = "cloudsearch.region";

    public CloudSearchConfig(Properties properties){
        super(properties);
        declareProperty(SEARCH_ENDPOINT, true);
        declareProperty(DOC_ENDPOINT, true);
        declareProperty(AWS_ACCESS_KEY_ID, null, false);
        declareProperty(AWS_SECRET_ACCESS_KEY, null, false);
        declareProperty(DEBUG, false, false);
        declareProperty(TIMEOUT, -1, false);
        declareProperty(MAX_RETRY_COUNT, -1, false);
        declareProperty(REGION, "us-west-2", false);
    }

    public String getSearchEndpoint(){
        return getString(SEARCH_ENDPOINT);
    }

    public String getDocEndpoint(){
        return getString(DOC_ENDPOINT);
    }

    public String getAccessKeyId(){
        return getString(AWS_ACCESS_KEY_ID);
    }

    public String getSecretKeyId(){
        return getString(AWS_SECRET_ACCESS_KEY);
    }

    public boolean getDebugFlag(){
        return getBoolean(DEBUG);
    }

    public int getSocketTimeout(){
        return getInteger(TIMEOUT);
    }

    public int getRetryCount(){
        return getInteger(MAX_RETRY_COUNT);
    }

    public String getRegion(){
        return getString(REGION);
    }
}