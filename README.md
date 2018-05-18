# SAP Receiver

This application was written to accept IDOCs from SAP, write the unmodified XML file to Azure Blob Storage, and to extract fields from the document and append them into a CSV in Azure Blob Storage.

## Azure Blob Storage

There are a few interesting things regarding the use of Azure Blob Storage:

* The unmodified XML files are written as Block Blobs.

* Each CSV row is appended to an Append Blob. This allows for very fast append operations even from multiple writers.

* All writes are committed asynchronously, but all must complete before a response code is sent.

* The CSV write is optimistic, meaning it attempts to commit the block whether or not the file exists. Most of the time the file exists and so no error happens. If the file does not exist, a call is made to create it and then append it. This results in 1 more service call if the file doesn't exist, but 1 fewer service call whenever it does (the most common case).

* Content extracted from the XML could be part of 0, 1, or many CSV schemas, resulting in the same number of write operations (CSV files).

* An operation to create a Blob that already exists normally wipes the existing content, but using the header "If-None-Match: *", we can get a 409 error instead.

* Multiple writers to an Append Blob is only supported in the .NET SDK, so this sample uses the Azure Storage REST API directly. When using multiple writers in this way, the **order the rows are committed to the CSV file is not guaranteed**.

* The code supports using either a SAS querystring (preferred) or a storage key.

## Configuration

There are a number of variables that must be set and some that can be optionally set. The variables may be set by any of the following methods:

1. Providing them on the command line.
2. Setting environmental variables.
3. Providing them in a .env file.
4. Use the default value, if there is one.

Those methods are in order based on their precident. For instance, if you set an environment variable in a .env file it can be overriden by specifying an environment variable to the console or by providing the variable via the command line.

The variables can be seen by running...

```bash
node server --help
```

...which will output...

```bash
  Usage: server [options]

  Options:

    -V, --version        output the version number
    -l, --log-level <s>  LOG_LEVEL. The minimum level to log to the console (error, warn, info, verbose, debug, silly). Defaults to "error".
    -d, --debug          Turn on debugging for the REST API calls.
    -p, --port <n>       PORT. The port to host the web services on. Defaults to 8080.
    -a, --account <s>    STORAGE_ACCOUNT. Required. The name of the Azure Storage Account.
    -c, --container <s>  STORAGE_CONTAINER. Required. The name of the Azure Storage Account Container.
    -s, --sas <s>        STORAGE_SAS. The Shared Access Signature querystring.
    -k, --key <s>        STORAGE_KEY. The Azure Storage Account key.
    -t, --period <s>     FOLDER_PERIOD. The period used to create the timeslice folders.
    -f, --format <s>     FOLDER_FORMAT. The format used for the timeslice folders.
    -h, --help           output usage information

Environment variables can be used instead of the command line options. The variable names are shown above.

The following variables must be set:
  STORAGE_ACCOUNT
  STORAGE_CONTAINER

One of the following must also be set:
  STORAGE_SAS
  STORAGE_KEY
```

## Shared Access Signature

As you can see in the screenshot below, the SAS needs only the abilty to write blobs.

![screenshot01](/images/screenshot01.png)

## Running

The dependencies can be installed like this:

```bash
npm install
```

The application can be run like this:

```bash
node server.js
```

You are safe to run as many of these processes as you need for redundancy and throughput. The method of writing to Append Blobs safely supports multiple writers.

To POST a document for processing, you can do something like this:

```bash
curl -X POST -d @./idoc.xml http://localhost:8080
```

## Process Flow

1. A POST request containing an XML body is received.
2. The timeslice folder name is determined.
3. The raw XML file is saved in the timeslice folder. A GUID is used as part of the name to ensure the filenames are unique.
4. The schemas are examined to determine if anything needs to be extracted to a CSV file.
5. Any extracted rows are written to the appropriate CSV files (Append Blobs).
6. The response is sent as 200 if all files were written and a 500 if there was an error.

Writing the files (steps 3 & 5) are asynchronous, but all writes must finish before the response (step 6) is sent.

## Schemas

In the "/schemas" folder, you can define one or more JSON files that determine how columns are extracted from the XML documents so that they can be put into the CSV file as rows.

Consider this example:

```json
{
    "name": "MATMA05",
    "filename": "matmas.csv",
    "identify": "/MATMAS05",
    "columns": [
        {
            "path": "/MATMAS05/IDOC/EDI_DC40/DOCNUM",
            "default": "0000000000000000",
            "enclosure": "\""
        },
        {
            "path": "/MATMAS05/IDOC/E1MARAM/E1MARCM/BESKZ",
        }
    ]
}
```

* name - This identifies the schema in the logs.
* filename - This can be a filename or a full path (ex. folder/folder/file.csv).
* identify - This is an XPATH string. If the XML document being examined returns 1 or more rows after executing that query, this schema will be used (ie. the row will be committed to the CSV file).
* columns - The columns (in order) as they will be committed to the CSV.
  * path - The XPATH string that will be used to identify the node. The inner content of that node will be extracted as the column value.
  * default - Optionally, you may specify a default value if the node isn't found. Otherwise, the default will be empty.
  * enclosure - Optionally, you may specify a string to be included at the start and end of the column value (as shown here, a ").

## CSV Partitioning

Given...

* Append Blobs cannot have more than 50,000 blocks
* Each write operation is a block
* Each row is written independently in this application (so there doesn't have to be any persistent caching)

...if you expect any of your CSV files to have more than 50,000 rows, you need to partition the file.

You can configure this in a schema like this:

```json
{
    "name": "MATMA05",
    "filename": "matmas-${partition}.csv",
    "partitions": 4,
    "identify": "/MATMAS05",
    "fields": [ ... ]
}
```

You will include the "${partition}" keyword somewhere in the filename and then specify the number of partitions. As the application matches data with this schema, it will round-robin the partition number such that in the example it would write to:

1. matmas-1.csv
2. matmas-2.csv
3. matmas-3.csv
4. matmas-4.csv
5. matmas-1.csv
6. matmas-2.csv, etc.

4 partitions, as shown in this example, could support up to 200,000 rows.