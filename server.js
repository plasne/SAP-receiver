// NOTE: Axios mucks with the headers, including adding "Accept" and "User-Agent"

// remove METHOD
// convert variables to upper case
// fix all methods to use try/catch and promise moved to just request
// move read files, etc. to promisify

// includes
require('dotenv').config();
const winston = require('winston');
const cmd = require('commander');
const fs = require('fs');
const agentKeepAlive = require('agentkeepalive');
const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const crypto = require('crypto');
const querystring = require('query-string');
const uuidv4 = require('uuid/v4');
const xpath = require('xpath');
const dom = require('xmldom').DOMParser;
const moment = require('moment');
require('moment-round');

// define command line parameters
cmd.version('0.1.0')
    .option(
        '-l, --log-level <s>',
        `LOG_LEVEL. The minimum level to log to the console (error, warn, info, verbose, debug, silly). Defaults to "error".`,
        /^(error|warn|info|verbose|debug|silly)$/i
    )
    .option('-d, --debug', 'Turn on debugging for the REST API calls.')
    .option(
        '-p, --port <n>',
        `PORT. The port to host the web services on. Defaults to "8080".`,
        parseInt
    )
    .option(
        '-a, --account <s>',
        `STORAGE_ACCOUNT. Required. The name of the Azure Storage Account.`
    )
    .option(
        '-c, --container <s>',
        `STORAGE_CONTAINER. Required. The name of the Azure Storage Account Container.`
    )
    .option(
        '-s, --sas <s>',
        `STORAGE_SAS. The Shared Access Signature querystring.`
    )
    .option('-k, --key <s>', `STORAGE_KEY. The Azure Storage Account key.`)
    .option(
        '-t, --period <s>',
        `FOLDER_PERIOD. The period used to create the timeslice folders. Defaults to "1 hour".`
    )
    .option(
        '-f, --format <s>',
        `FOLDER_FORMAT. The format used for the timeslice folders. Defaults to "YYYYMMDDTHHmmss".`
    )
    .option(
        '-h, --schemas <s>',
        `SCHEMAS. The path to the folder containing the schemas you want to apply or "none". Defaults to "./schemas".`
    )
    .on('--help', _ => {
        console.log('');
        console.log(
            'Environment variables can be used instead of the command line options. The variable names are shown above.'
        );
        console.log('');
        console.log('The following variables must be set:');
        console.log('  STORAGE_ACCOUNT');
        console.log('  STORAGE_CONTAINER');
        console.log('');
        console.log('One of the following must also be set:');
        console.log('  STORAGE_SAS');
        console.log('  STORAGE_KEY');
        console.log('');
    })
    .parse(process.argv);

// globals
const logLevel = cmd.logLevel || process.env.LOG_LEVEL || 'error';
const port = cmd.port || process.env.PORT || 8080;
const account = cmd.account || process.env.STORAGE_ACCOUNT;
const container = cmd.container || process.env.STORAGE_CONTAINER;
const sas = cmd.sas || process.env.STORAGE_SAS;
const key = cmd.key || process.env.STORAGE_KEY;
const period = cmd.period || process.env.FOLDER_PERIOD || '1 hour';
const format = cmd.format || process.env.FOLDER_FORMAT || 'YYYYMMDDTHHmmss';
const SCHEMAS = cmd.schemas || process.env.SCHEMAS || './schemas';

// enable logging
const logColors = {
    error: '\x1b[31m', // red
    warn: '\x1b[33m', // yellow
    info: '', // white
    verbose: '\x1b[32m', // green
    debug: '\x1b[32m', // green
    silly: '\x1b[32m' // green
};
const logger = winston.createLogger({
    level: logLevel,
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.printf(event => {
                    const color = logColors[event.level] || '';
                    const level = event.level.padStart(7);
                    if (event.coorelationId) {
                        return `${event.timestamp} ${color}${level}\x1b[0m ${
                            event.coorelationId
                        }: ${event.message}`;
                    } else {
                        return `${event.timestamp} ${color}${level}\x1b[0m: ${
                            event.message
                        }`;
                    }
                })
            )
        })
    ]
});

// log startup
console.log(`Log level set to "${logLevel}".`);
if (cmd.debug) require('request-debug')(request);
logger.log('verbose', `STORAGE_ACCOUNT = "${account}".`);
logger.log('verbose', `STORAGE_CONTAINER = "${container}".`);
logger.log('verbose', `STORAGE_KEY is ${key ? 'defined' : 'undefined'}.`);
logger.log('verbose', `STORAGE_SAS is ${sas ? 'defined' : 'undefined'}.`);
logger.log('verbose', `FOLDER_PERIOD = "${period}".`);
logger.log('verbose', `FOLDER_FORMAT = "${format}".`);
logger.log('verbose', `SCHEMAS = "${SCHEMAS}".`);

// use an HTTP(s) agent with keepalive and connection pooling
const agent = new agentKeepAlive.HttpsAgent({
    maxSockets: 40,
    maxFreeSockets: 10,
    timeout: 60000,
    freeSocketKeepAliveTimeout: 30000
});

// start up express
const app = express();
app.use(
    bodyParser.text({
        type: () => {
            return true;
        }
    })
);

String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.replace(new RegExp(search, 'g'), replacement);
};

function generateSignature(method, path, options, loggerTags) {
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
        if (key.substring(0, 5) === 'x-ms-') {
            xheaders.push(`${key}:${options.headers[key]}`);
        }
    }
    xheaders.sort((a, b) => a.localeCompare(b));

    // zero length for the body is an empty string, not 0
    const len = options.body ? Buffer.byteLength(options.body) : '';

    // potential content-type, if-none-match
    const ct = options.headers['Content-Type'] || '';
    const none = options.headers['If-None-Match'] || '';

    // generate the signature line
    let raw = `${method}\n\n\n${len}\n\n${ct}\n\n\n\n${none}\n\n\n${xheaders.join(
        '\n'
    )}\n/${account}/${container}`;
    if (path) raw += `/${path}`;
    raw += parameters.length > 0 ? `\n${parameters.join('\n')}` : '';
    logger.log(
        'debug',
        `The unencoded signature is "${raw.replaceAll('\n', '\\n')}"`,
        loggerTags
    );

    // sign it
    const hmac = crypto.createHmac('sha256', new Buffer.from(key, 'base64'));
    const signature = hmac.update(raw, 'utf-8').digest('base64');

    // return the Authorization header
    return `SharedKey ${account}:${signature}`;
}

function createAppendBlob(filename, loggerTags, http409IsLoggedAsError = true) {
    return new Promise((resolve, reject) => {
        logger.log(
            'verbose',
            `creating append blob "${filename}"...`,
            loggerTags
        );

        // specify the request options, including the headers
        const options = {
            agent: agent,
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${sas ||
                ''}`,
            headers: {
                'x-ms-version': '2017-07-29',
                'x-ms-date': new Date().toUTCString(),
                'x-ms-blob-type': 'AppendBlob',
                'Content-Type': 'text/plain; charset=UTF-8',
                'If-None-Match': '*' // ensures the blob cannot be created more than once
            }
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(
                'PUT',
                filename,
                options,
                loggerTags
            );
            options.headers.Authorization = signature;
        }

        // execute
        request.put(options, (error, response, body) => {
            if (
                !error &&
                response.statusCode >= 200 &&
                response.statusCode < 300
            ) {
                logger.log(
                    'verbose',
                    `created append blob "${filename}".`,
                    loggerTags
                );
                resolve();
            } else if (error) {
                logger.error(
                    `failed to create append blob "${filename}": ${error}`,
                    loggerTags
                );
                reject(error);
            } else if (response.statusCode === 409 && !http404IsLoggedAsError) {
                logger.info(
                    `failed to create append blob "${filename}" because it already exists: ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            } else {
                logger.error(
                    `failed to create append blob "${filename}": ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            }
        });
    });
}

function appendToBlob(
    filename,
    row,
    loggerTags,
    http404IsLoggedAsError = true
) {
    return new Promise((resolve, reject) => {
        logger.log('verbose', `appending to blob "${filename}"...`, loggerTags);

        // specify the request options, including the headers
        const options = {
            agent: agent,
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${
                sas ? sas + '&' : '?'
            }comp=appendblock`,
            headers: {
                'x-ms-version': '2017-07-29',
                'x-ms-date': new Date().toUTCString(),
                'x-ms-blob-type': 'AppendBlob'
            },
            body: `${row}\n`
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(
                'PUT',
                filename,
                options,
                loggerTags
            );
            options.headers.Authorization = signature;
        }

        // execute
        request.put(options, (error, response, body) => {
            if (
                !error &&
                response.statusCode >= 200 &&
                response.statusCode < 300
            ) {
                logger.log(
                    'verbose',
                    `appended blob "${filename}".`,
                    loggerTags
                );
                resolve();
            } else if (error) {
                logger.error(
                    `failed to append blob "${filename}": ${error}`,
                    loggerTags
                );
                reject(error);
            } else if (response.statusCode === 404 && !http404IsLoggedAsError) {
                logger.info(
                    `failed to append blob "${filename}" which doesn't exist: ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            } else {
                logger.error(
                    `failed to append blob "${filename}": ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            }
        });
    });
}

async function appendToBlobWithCreate(filename, row, headers, loggerTags) {
    // optimistically try and write the record
    try {
        await appendToBlob(filename, row, loggerTags, false);
    } catch (ex) {
        if (ex.message.substring(0, 4) === '404:') {
            // the blob doesn't exist, create it and then try the write again
            try {
                await createAppendBlob(filename, loggerTags, false);
                if (headers) appendToBlob(filename, headers, loggerTags);
                await appendToBlob(filename, row, loggerTags);
            } catch (ex) {
                if (ex.message.substring(0, 4) === '409:') {
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

async function createBlockBlob(filename, body, metadata, loggerTags) {
    try {
        logger.log('verbose', `creating block blob "${filename}"...`);

        // specify the request options, including the headers
        const options = {
            method: 'PUT',
            agent: agent,
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${sas ||
                ''}`,
            headers: {
                'x-ms-version': '2017-07-29',
                'x-ms-date': new Date().toUTCString(),
                'x-ms-blob-type': 'BlockBlob',
                'Content-Type': 'application/xml'
            },
            body: body
        };

        // add metadata
        if (metadata) {
            for (const name in metadata) {
                options.headers[`x-ms-meta-${name}`] = metadata[name];
            }
        }

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(
                'PUT',
                filename,
                options,
                loggerTags
            );
            options.headers.Authorization = signature;
        }

        // commit
        await new Promise((resolve, reject) => {
            request(options, (error, response, body) => {
                if (
                    !error &&
                    response.statusCode >= 200 &&
                    response.statusCode < 300
                ) {
                    logger.log(
                        'verbose',
                        `created block blob "${filename}".`,
                        loggerTags
                    );
                    resolve(body);
                } else if (error) {
                    reject(error);
                } else {
                    reject(
                        new Error(
                            `HTTP response ${response.statusCode}: ${
                                response.statusMessage
                            }`
                        )
                    );
                }
            });
        });
    } catch (error) {
        logger.error(`failed to create block blob "${filename}"`, loggerTags);
        throw error;
    }
}

function readBlockBlob(filename, loggerTags) {
    return new Promise((resolve, reject) => {
        logger.log('verbose', `reading block blob "${filename}"...`);

        // specify the request options, including the headers
        const options = {
            agent: agent,
            url: `https://${account}.blob.core.windows.net/${container}/${filename}${sas ||
                ''}`,
            headers: {
                'x-ms-version': '2017-07-29',
                'x-ms-date': new Date().toUTCString()
            }
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(
                'GET',
                filename,
                options,
                loggerTags
            );
            options.headers.Authorization = signature;
        }

        // execute
        request.get(options, (error, response, body) => {
            if (
                !error &&
                response.statusCode >= 200 &&
                response.statusCode < 300
            ) {
                logger.log(
                    'verbose',
                    `successfully read block blob "${filename}".`,
                    loggerTags
                );
                resolve(body);
            } else if (error) {
                logger.error(
                    `failed to read block blob "${filename}": ${error}`,
                    loggerTags
                );
                reject(error);
            } else {
                logger.error(
                    `failed to read block blob "${filename}": ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            }
        });
    });
}

function getUnprocessedBlobList(period, loggerTags, unprocessed = [], marker) {
    return new Promise((resolve, reject) => {
        logger.log('verbose', `listing blobs for period "${period}"...`);

        // specify the request options, including the headers
        const options = {
            agent: agent,
            url: `https://${account}.blob.core.windows.net/${container}${
                sas ? sas + '&' : '?'
            }restype=container&comp=list&prefix=${period +
                '/'}&include=metadata${marker ? '&marker=' + marker : ''}`,
            headers: {
                'x-ms-version': '2017-07-29',
                'x-ms-date': new Date().toUTCString()
            }
        };

        // generate and apply the signature
        if (!sas) {
            const signature = generateSignature(
                'GET',
                null,
                options,
                loggerTags
            );
            options.headers.Authorization = signature;
        }

        // execute
        request.get(options, (error, response, body) => {
            if (
                !error &&
                response.statusCode >= 200 &&
                response.statusCode < 300
            ) {
                const doc = new dom().parseFromString(body);

                // look for unprocessed XML files
                for (blob of xpath.select(
                    '/EnumerationResults/Blobs/Blob',
                    doc
                )) {
                    const filename = xpath.select1('string(Name)', blob);
                    if (
                        filename.endsWith('.xml') &&
                        xpath.select1('string(Metadata/processed)', blob) ===
                            'false'
                    ) {
                        unprocessed.push(filename);
                        logger.log(
                            'debug',
                            `found unprocessed file: "${filename}"`,
                            loggerTags
                        );
                    }
                }

                // iterate if there is more to grab
                const next = xpath.select1(
                    'string(/EnumerationResults/NextMarker)',
                    doc
                );
                if (next) {
                    logger.log(
                        'verbose',
                        `fetching more block blobs for period "${period}..."`,
                        loggerTags
                    );
                    getUnprocessedBlockBlobList(
                        period,
                        loggerTags,
                        unprocessed,
                        next
                    )
                        .then(_ => {
                            resolve(unprocessed);
                        })
                        .catch(error => {
                            reject(error);
                        });
                } else {
                    logger.log(
                        'verbose',
                        `all block blobs for period "${period}" were examined: ${
                            unprocessed.length
                        } unprocessed.`,
                        loggerTags
                    );
                    resolve(unprocessed);
                }
            } else if (error) {
                logger.error(
                    `failed to list block blobs "${period}": ${error}`,
                    loggerTags
                );
                reject(error);
            } else {
                logger.error(
                    `failed to list block blobs "${period}": ${
                        response.statusCode
                    }: ${response.statusMessage}`,
                    loggerTags
                );
                reject(
                    new Error(
                        `${response.statusCode}: ${response.statusMessage}`
                    )
                );
            }
        });
    });
}

function readFile(path) {
    return new Promise((resolve, reject) => {
        fs.readFile(path, 'utf-8', (error, content) => {
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
        logger.log('verbose', `loading ${filename}...`);
        const raw = await readFile(`${path}/${filename}`);
        const json = JSON.parse(raw);
        list.push(json);
        logger.log('verbose', `loaded ${filename}...`);
    }
    return list;
}

// this method returns a collection of promises to apply the schemas
function applySchemas(doc, schemas, period, addHeaders, loggerTags) {
    const promises = [];

    for (const schema of schemas) {
        if (xpath.select(schema.identify, doc).length > 0) {
            logger.log(
                'verbose',
                `schema identified as "${schema.name}".`,
                loggerTags
            );

            // extract the columns
            const columns = [];
            const row = [];
            for (const column of schema.columns) {
                columns.push(column.header || '');
                const enclosure = column.enclosure || '';
                const _default = column.default || '';
                const value = xpath.select1(`string(${column.path})`, doc);
                if (value) {
                    row.push(`${enclosure}${value}${enclosure}`);
                } else {
                    row.push(`${enclosure}${_default}${enclosure}`);
                }
            }

            // determine the filename
            let partition = schema.partition || 1;
            const filename = schema.filename.replace('${partition}', partition);
            partition++;
            if (partition > (schema.partitions || 1)) partition = 1;
            schema.partition = partition;

            // append to a CSV
            logger.log('debug', `row: ${row.join(',')}`, loggerTags);
            const headers = addHeaders ? columns.join(',') : null;
            const csv = appendToBlobWithCreate(
                `${period}/${filename}`,
                row.join(','),
                headers,
                loggerTags
            );
            promises.push(csv);
        }
    }

    return promises;
}

// startup
(async () => {
    try {
        // load all schemas
        const schemas =
            SCHEMAS.toLowerCase() === 'none'
                ? null
                : await readAllFiles(SCHEMAS);

        // accept documents via POST
        app.post('/', async (req, res) => {
            const coorelationId = uuidv4();
            logger.log('debug', `POST requested from ${req.ip}...`, {
                coorelationId: coorelationId
            });
            try {
                const promises = [];

                // determine the timeslice to apply the files to
                const now = new moment();
                const period_array = period.split(' ');
                const period_last = now.floor(
                    Number.parseInt(period_array[0]),
                    period_array[1]
                );
                const period_path = period_last.utc().format(format);

                // promise to save the raw file
                const raw = createBlockBlob(
                    `${period_path}/name-${uuidv4()}.xml`,
                    req.body,
                    { processed: false },
                    { coorelationId: coorelationId }
                );
                promises.push(raw);

                // promise to apply any schemas to the XML (no headers)
                if (schemas && schemas.length > 0) {
                    const doc = new dom().parseFromString(req.body);
                    const csvs = applySchemas(
                        doc,
                        schemas,
                        period_path,
                        false,
                        { coorelationId: coorelationId }
                    );
                    for (const csv of csvs) {
                        promises.push(csv);
                    }
                }

                // respond when all promises are fulfilled
                await Promise.all(promises);
                logger.log('debug', `POST response is 200.`, {
                    coorelationId: coorelationId
                });
                res.status(200).end();
            } catch (error) {
                // handle any exceptions
                logger.log('debug', `POST response is 500.`, {
                    coorelationId: coorelationId
                });
                res.status(500).end();
                logger.error(error.stack);
            }
        });
    } catch (error) {
        logger.error(`aborted due to error in startup process.`);
        logger.error(error.stack);
        process.exit(1);
    }

    // start listening
    app.listen(port, () => {
        logger.log('info', `Listening on port ${port}...`);
    });
})();
