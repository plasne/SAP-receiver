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

1. A request comes in 