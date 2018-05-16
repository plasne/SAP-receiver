
// includes
const fs = require("fs");
const config = require("config");
const express = require("express");
const bodyParser = require("body-parser");
const request = require("request");
const crypto = require("crypto");
const querystring = require("query-string");
const uuidv4 = require("uuid/v4");
const xpath = require("xpath");
const dom = require("xmldom").DOMParser;
const moment = require("moment");
require("moment-round");

//const storage = require("azure-storage");
//const blobService = storage.createBlobServiceWithSas(host, sas);

require('request-debug')(request);

// globals
const account = config.get("account");
const container = config.get("container");
const key = (config.has("key")) ? config.get("key") : null;
const sas = null; //(config.has("sas")) ? config.get("sas") : null;
const period = config.get("period");
const format = (config.has("format")) ? config.get("format") : "YYYYMMDDTHHmmss";

// start up express
const app = express();
app.use(bodyParser.text({
    type: () => { return true; }
}));

function generateSignature(filename, options) {

    // pull out all querystring parameters so they can be sorted and used in the signature
    const parameters = [];
    const parsed = querystring.parseUrl(options.url);
    for (const key in parsed.query) {
        parameters.push(`${key}:${parsed.query[key]}`);
    }
    parameters.sort((a, b) => a.localeCompare(b));

    // pull out all x-ms- headers so they can be sorted and used in the signature
    const xheaders = [];
    for (const key in options.headers) {
        if (key.substring(0, 5) === "x-ms-") {
            xheaders.push(`${key}:${options.headers[key]}`);
        }
    }
    xheaders.sort((a, b) => a.localeCompare(b));

    // zero length for the body is an empty string, not 0
    const len = (options.body) ? Buffer.byteLength(options.body) : "";

    // potential content-type, if-none-match
    const ct = options.headers["Content-Type"] || "";
    const none = options.headers["If-None-Match"] || "";

    // generate the signature line and sign it
    let raw = `PUT\n\n\n${len}\n\n${ct}\n\n\n\n${none}\n\n\n${xheaders.join("\n")}\n/${account}/${container}/${filename}`;
    raw += (parameters.length > 0) ? `\n${parameters.join("\n")}` : "";
    const hmac = crypto.createHmac("sha256", new Buffer.from(key, "base64"));
    const signature = hmac.update(raw, "utf-8").digest("base64");
    
    // return the Authorization header
    return `SharedKey ${account}:${signature}`;

}

function createAppendBlob(filename) {
    return new Promise((resolve, reject) => {
        console.log(`creating append blob "${filename}"...`);

        // specify the request options, including the headers
        const options = {
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${sas || ""}`,
            headers: {
                "x-ms-version": "2017-07-29",
                "x-ms-date": (new Date()).toUTCString(),
                "x-ms-blob-type": "AppendBlob",
                "Content-Type": "text/plain; charset=UTF-8",
                "If-None-Match": "*" // ensures the blob cannot be created more than once
            }
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(filename, options);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                console.log(`created append blob "${filename}".`);
                resolve();
            } else if (error) {
                console.log(`failed to create append blob "${filename}": ${error}`);
                reject(error);
            } else {
                console.log(`failed to create append blob "${filename}": ${response.statusCode}: ${response.statusMessage}`);
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

function appendToBlob(filename, row) {
    return new Promise((resolve, reject) => {

        // specify the request options, including the headers
        const options = {
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${(sas) ? sas + "&" : "?"}comp=appendblock`,
            headers: {
                "x-ms-version": "2017-07-29",
                "x-ms-date": (new Date()).toUTCString(),
                "x-ms-blob-type": "AppendBlob"
            },
            body: `${row}\n`
        };
        
        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(filename, options);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                resolve();
            } else if (error) {
                reject(error);
            } else {
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

async function appendToBlobWithCreate(filename, row) {

    // optimistically try and write the record
    try {
        await appendToBlob(filename, row);
    } catch (ex) {
        if (ex.message.substring(0, 4) === "404:") {

            // the blob doesn't exist, create it and then try the write again
            try {
                await createAppendBlob(filename);
                await appendToBlob(filename, row);
            } catch (ex) {
                if (ex.message.substring(0, 4) === "409:") {

                    // the blob was created by another process in a race condition
                    await appendToBlob(filename, row);

                } else {
                    throw ex;
                }
            }

        } else {
            throw ex;
        }
    }

}

function createBlockBlob(filename, body) {
    return new Promise((resolve, reject) => {

        // specify the request options, including the headers
        const options = {
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${sas || ""}`,
            headers: {
                "x-ms-version": "2017-07-29",
                "x-ms-date": (new Date()).toUTCString(),
                "x-ms-blob-type": "BlockBlob",
                "Content-Type": "application/xml"
            },
            body: body
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(filename, options);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                resolve();
            } else if (error) {
                reject(error);
            } else {
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

function readFile(path) {
    return new Promise((resolve, reject) => {
        fs.readFile(path, "utf-8", (error, content) => {
            if (!error) {
                resolve(content);
            } else {
                reject(error);
            }
        });
    });
}

function readFolder(path) {
    return new Promise((resolve, reject) => {
        fs.readdir(path, (error, files) => {
            if (!error) {
                resolve(files);
            } else {
                reject(error);
            }
        });
    });
}

async function readAllFiles(path) {
    const list = [];
    const filenames = await readFolder(path);
    for (const filename of filenames) {
        console.log(`loading ${filename}...`);
        const raw = await readFile(`${path}/${filename}`);
        const json = JSON.parse(raw);
        list.push(json);
        console.log(`loaded ${filename}.`);
    }
    return list;
}

// load all schemas
readAllFiles("./schemas")
.then(schemas => {

    // accept documents
    app.post("/", (req, res) => {
        const promises = [];

        // determine the timeslice to apply the files to
        const now = new moment();
        const period_array = period.split(" ");
        const period_last = now.floor(Number.parseInt(period_array[0]), period_array[1]);
        const period_path = period_last.utc().format(format);
        
        // save the raw file
        const raw = createBlockBlob(`${period_path}/name-${uuidv4()}.xml`, req.body);
        promises.push(raw);

        // parse the IDOC
        const doc = new dom().parseFromString(req.body);

        // find any matching schemas
        for (const schema of schemas) {
            if (xpath.select(schema.identify, doc).length > 0) {

                // extract the fields
                const row = [];
                for (const field of schema.fields) {
                    const enclosure = field.enclosure || "";
                    const _default = field.default || "";
                    const nodes = xpath.select(field.path, doc);
                    if (nodes.length > 0) {
                        row.push(`${enclosure}${nodes[0].firstChild.data}${enclosure}`); //nodes[0].localName is the node name
                    } else {
                        row.push(`${enclosure}${_default}${enclosure}`);
                    }
                }

                // append to a CSV
                const csv = appendToBlobWithCreate(`${period_path}/${schema.filename}`, row.join(","));
                promises.push(csv);
                
            }
        }

        // wait for all writes to finish
        Promise.all(promises)
        .then(_ => {
            res.status(200).end();
        })
        .catch(ex => {
            res.status(500).end();
            console.error(ex);
        });

    });

    // start listening
    const port = process.env.PORT || 8080;
    app.listen(port, () => {
        console.log(`Listening on port ${port}...`);
    });

})
.catch(ex => {
    console.error(ex);
});
