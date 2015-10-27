<!--
Copyright (c) 2012 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Cloudsearch running locally. 

### 1. Set Up YCSB

Clone the chookah-ycsb git repository and compile:

    git clone git://github.com/okoye/chookah-ycsb.git
    cd chookah-ycsb
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load cloudsearch -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run cloudsearch -s -P workloads/workloada

For further configuration see below: 

### Defaults Configuration
The default setting for the CloudSearch node that is created is as follows:

    cloudsearch.search.endpoint=search-foo.us-east-1.cloudsearch.amazonaws.com
    cloudsearch.doc.endpoint=doc-foo.us-east-1.cloudsearch.amazonaws.com
    cloudsearch.debug=false
    cloudsearch.sockettimeout=20000 (in millisecs)
    cloudsearch.retrycount=0 (retries when 5XX errors occur)
    cloudsearch.api=2011
    aws.accesskey=foo
    aws.secretkey=bar
    aws.region=us-east-1
