
// includes
require("dotenv").config();
const winston = require("winston");
const cmd = require("commander");
const fs = require("fs");
const agentKeepAlive = require("agentkeepalive");
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

// define command line parameters
cmd
    .version("0.1.0")
    .option("-l, --log-level <s>", `LOG_LEVEL. The minimum level to log to the console (error, warn, info, verbose, debug, silly). Defaults to "error".`, /^(error|warn|info|verbose|debug|silly)$/i)
    .option("-d, --debug", "Turn on debugging for the REST API calls.")
    .option("-p, --port <n>", `PORT. The port to host the web services on. Defaults to "8080".`, parseInt)
    .option("-a, --account <s>", `STORAGE_ACCOUNT. Required. The name of the Azure Storage Account.`)
    .option("-c, --container <s>", `STORAGE_CONTAINER. Required. The name of the Azure Storage Account Container.`)
    .option("-s, --sas <s>", `STORAGE_SAS. The Shared Access Signature querystring.`)
    .option("-k, --key <s>", `STORAGE_KEY. The Azure Storage Account key.`)
    .option("-t, --period <s>", `FOLDER_PERIOD. The period used to create the timeslice folders. Defaults to "1 hour".`)
    .option("-f, --format <s>", `FOLDER_FORMAT. The format used for the timeslice folders. Defaults to "YYYYMMDDTHHmmss".`)
    .on("--help", _ => {
        console.log("");
        console.log("Environment variables can be used instead of the command line options. The variable names are shown above.");
        console.log("");
        console.log("The following variables must be set:");
        console.log("  STORAGE_ACCOUNT");
        console.log("  STORAGE_CONTAINER");
        console.log("");
        console.log("One of the following must also be set:");
        console.log("  STORAGE_SAS");
        console.log("  STORAGE_KEY");
        console.log("");
    })
    .parse(process.argv);

// globals
const logLevel  = cmd.logLevel  || process.env.LOG_LEVEL          || "error";
const port      = cmd.port      || process.env.PORT               || 8080;
const account   = cmd.account   || process.env.STORAGE_ACCOUNT;
const container = cmd.container || process.env.STORAGE_CONTAINER;
const sas       = cmd.sas       || process.env.STORAGE_SAS;
const key       = cmd.key       || process.env.STORAGE_KEY;
const period    = cmd.period    || process.env.FOLDER_PERIOD      || "1 hour";
const format    = cmd.format    || process.env.FOLDER_FORMAT      || "YYYYMMDDTHHmmss";

// enable logging
const logger = winston.createLogger({
    level: logLevel,
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.printf(event => {
                    const color = ((level) => {
                        switch (level) {
                            case "error":   return "\x1b[31m"; // red
                            case "warn":    return "\x1b[33m"; // yellow
                            case "info":    return "";         // white
                            case "verbose": return "\x1b[32m"; // green
                            case "debug":   return "\x1b[32m"; // green
                            case "silly":   return "\x1b[32m"; // green
                            default:        return "";         // white
                        }
                    })(event.level);
                    const level = event.level.padStart(7);
                    if (event.coorelationId) {
                        return `${event.timestamp} ${color}${level}\x1b[0m ${event.coorelationId}: ${event.message}`;
                    } else {
                        return `${event.timestamp} ${color}${level}\x1b[0m: ${event.message}`;
                    }
                })
            )
        })
    ]
});

// log startup
console.log(`Log level set to "${logLevel}".`);
if (cmd.debug) require('request-debug')(request);
logger.log("verbose", `account = "${account}".`);
logger.log("verbose", `container = "${container}".`);
logger.log("verbose", `key is ${(key) ? "defined" : "undefined"}.`);
logger.log("verbose", `sas is ${(key) ? "defined" : "undefined"}.`);
logger.log("verbose", `period = "${period}".`);
logger.log("verbose", `format = "${format}".`);

// use an HTTP(s) agent with keepalive and connection pooling
const agent = new agentKeepAlive.HttpsAgent({
    maxSockets: 40,
    maxFreeSockets: 10,
    timeout: 60000,
    freeSocketKeepAliveTimeout: 30000
});

// start up express
const app = express();
app.use(bodyParser.text({
    type: () => { return true; }
}));

String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.replace(new RegExp(search, "g"), replacement);
};

function generateSignature(filename, options, loggerTags) {

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

    // generate the signature line
    let raw = `PUT\n\n\n${len}\n\n${ct}\n\n\n\n${none}\n\n\n${xheaders.join("\n")}\n/${account}/${container}/${filename}`;
    raw += (parameters.length > 0) ? `\n${parameters.join("\n")}` : "";
    logger.log("debug", `The unencoded signature is "${raw.replaceAll("\n", "\\n")}"`, loggerTags);

    // sign it
    const hmac = crypto.createHmac("sha256", new Buffer.from(key, "base64"));
    const signature = hmac.update(raw, "utf-8").digest("base64");
    
    // return the Authorization header
    return `SharedKey ${account}:${signature}`;

}

function createAppendBlob(filename, loggerTags, http409IsLoggedAsError = true) {
    return new Promise((resolve, reject) => {
        logger.log("verbose", `creating append blob "${filename}"...`, loggerTags);

        // specify the request options, including the headers
        const options = {
            agent: agent,
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
            const signature = generateSignature(filename, options, loggerTags);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                logger.log("verbose", `created append blob "${filename}".`, loggerTags);
                resolve();
            } else if (error) {
                logger.error(`failed to create append blob "${filename}": ${error}`, loggerTags);
                reject(error);
            } else if (response.statusCode === 409 && !http404IsLoggedAsError) {
                logger.info(`failed to create append blob "${filename}" because it already exists: ${response.statusCode}: ${response.statusMessage}`, loggerTags);
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            } else {
                logger.error(`failed to create append blob "${filename}": ${response.statusCode}: ${response.statusMessage}`, loggerTags);
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

function appendToBlob(filename, row, loggerTags, http404IsLoggedAsError = true) {
    return new Promise((resolve, reject) => {
        logger.log("verbose", `appending to blob "${filename}"...`, loggerTags);

        // specify the request options, including the headers
        const options = {
            agent: agent,
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
            const signature = generateSignature(filename, options, loggerTags);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                logger.log("verbose", `appended blob "${filename}".`, loggerTags);
                resolve();
            } else if (error) {
                logger.error(`failed to append blob "${filename}": ${error}`, loggerTags);
                reject(error);
            } else if (response.statusCode === 404 && !http404IsLoggedAsError) {
                logger.info(`failed to append blob "${filename}" which doesn't exist: ${response.statusCode}: ${response.statusMessage}`, loggerTags);
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            } else {
                logger.error(`failed to append blob "${filename}": ${response.statusCode}: ${response.statusMessage}`, loggerTags);
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

async function appendToBlobWithCreate(filename, row, loggerTags) {

    // optimistically try and write the record
    try {
        await appendToBlob(filename, row, loggerTags, false);
    } catch (ex) {
        if (ex.message.substring(0, 4) === "404:") {

            // the blob doesn't exist, create it and then try the write again
            try {
                await createAppendBlob(filename, loggerTags, false);
                await appendToBlob(filename, row, loggerTags);
            } catch (ex) {
                if (ex.message.substring(0, 4) === "409:") {

                    // the blob was created by another process in a race condition
                    await appendToBlob(filename, row, loggerTags);

                } else {
                    throw ex;
                }
            }

        } else {
            throw ex;
        }
    }

}

function createBlockBlob(filename, body, loggerTags) {
    return new Promise((resolve, reject) => {
        logger.log("verbose", `creating block blob "${filename}"...`);

        // specify the request options, including the headers
        const options = {
            agent: agent,
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
            const signature = generateSignature(filename, options, loggerTags);
            options.headers.Authorization = signature;
        }
        
        // execute
        request.put(options, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                logger.log("verbose", `created block blob "${filename}".`, loggerTags);
                resolve();
            } else if (error) {
                logger.error(`failed to create block blob "${filename}": ${error}`, loggerTags);
                reject(error);
            } else {
                logger.error(`failed to create block blob "${filename}": ${response.statusCode}: ${response.statusMessage}`, loggerTags);
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
        logger.log("verbose", `loading ${filename}...`);
        const raw = await readFile(`${path}/${filename}`);
        const json = JSON.parse(raw);
        list.push(json);
        logger.log("verbose", `loaded ${filename}...`);
    }
    return list;
}

// load all schemas
readAllFiles("./schemas")
.then(schemas => {

    // accept documents
    app.post("/", async (req, res) => {
        const coorelationId = uuidv4();
        logger.log("debug", `POST requested from ${req.ip}...`, { coorelationId: coorelationId });
        try {
            const promises = [];
    
            // determine the timeslice to apply the files to
            const now = new moment();
            const period_array = period.split(" ");
            const period_last = now.floor(Number.parseInt(period_array[0]), period_array[1]);
            const period_path = period_last.utc().format(format);
            
            // save the raw file
            const raw = createBlockBlob(`${period_path}/name-${uuidv4()}.xml`, req.body, { coorelationId: coorelationId });
            promises.push(raw);
    
            // parse the IDOC
            const doc = new dom().parseFromString(req.body);
    
            // find any matching schemas
            for (const schema of schemas) {
                if (xpath.select(schema.identify, doc).length > 0) {
                    logger.log("verbose", `schema identified as "${schema.name}".`, { coorelationId: coorelationId });
    
                    // extract the fields
                    const row = [];
                    for (const field of schema.fields) {
                        const enclosure = field.enclosure || "";
                        const _default = field.default || "";
                        const nodes = xpath.select(field.path, doc);
                        const column = ((nodes) => {
                            if (nodes.length > 0) {
                                return `${enclosure}${nodes[0].firstChild.data}${enclosure}`;
                            } else {
                                return `${enclosure}${_default}${enclosure}`;
                            }
                        })(nodes);    
                        row.push(column);
                    }
    
                    // append to a CSV
                    logger.log("debug", `row: ${row.join(",")}`, { coorelationId: coorelationId });
                    const csv = appendToBlobWithCreate(`${period_path}/${schema.filename}`, row.join(","), { coorelationId: coorelationId });
                    promises.push(csv);
                    
                }
            }
    
            // wait for all writes to finish
            await Promise.all(promises);
            logger.log("debug", `POST response is 200.`, { coorelationId: coorelationId });
            res.status(200).end();

        } catch (ex) {

            // handle any exceptions
            logger.log("debug", `POST response is 500.`, { coorelationId: coorelationId });
            res.status(500).end();
            logger.error(ex.stack);

        }
    });

    // start listening
    app.listen(port, () => {
        logger.log("info", `Listening on port ${port}...`);
    });

})
.catch(ex => {
    logger.error(ex.stack);
});
