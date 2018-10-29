
// includes
//require("dotenv").config();
const cmd = require("commander");
const uuidv4 = require("uuid/v4");
const loremIpsum = require("lorem-ipsum");
const request = require("request");
const agentKeepAlive = require("agentkeepalive");
const readline = require("readline");

// define command line parameters
cmd
    .version("0.1.0")
    .option("-u, --url <s>", `URL. The URL of the receiver.`)
    .option("-i, --interval <i>", `INTERVAL. Send a request very "i" milliseconds. Defaults to "1000" ms.`, parseInt)
    .parse(process.argv);

// globals
const URL      = cmd.url      || process.env.URL;
const INTERVAL = cmd.interval || process.env.INTERVAL || 1000;

// validation
if (!URL) throw new Error("You must specify a URL for the receiver.");

// use an HTTP(s) agent with keepalive and connection pooling
const agent = (URL.toLowerCase().startsWith("https://")) ? new agentKeepAlive.HttpsAgent() : new agentKeepAlive();
agent.maxSockets = 40;
agent.maxFreeSockets = 10;
agent.timeout = 60000;
agent.freeSocketKeepAliveTimeout = 30000;

// counters
let success = 0;
let fail = 0;

// generate an XML document
function generateXML() {
    let x = "<doc>\n";
    x += `  <id>${uuidv4()}</id>\n`;
    x += `  <name>${loremIpsum({ count: 1, units: "word", format: "plain" })}</name>\n`;
    x += `  <c0>${loremIpsum({ count: 1, units: "word", format: "plain" })}</c0>\n`;
    x += `  <c1>${loremIpsum({ count: 1, units: "word", format: "plain" })}</c1>\n`;
    x += `  <c2>${loremIpsum({ count: 1, units: "word", format: "plain" })}</c2>\n`;
    x += `  <c3>${loremIpsum({ count: 1, units: "word", format: "plain" })}</c3>\n`;
    x += `  <short>${loremIpsum({ count: 1, units: "sentence", format: "plain" })}</short>\n`;
    x += `  <a0>${loremIpsum({ count: 1, units: "paragraph", format: "plain" })}</a0>\n`;
    x += `  <a1>${loremIpsum({ count: 1, units: "paragraph", format: "plain" })}</a1>\n`;
    x += `  <a2>${loremIpsum({ count: 1, units: "paragraph", format: "plain" })}</a2>\n`;
    x += `  <a3>${loremIpsum({ count: 1, units: "paragraph", format: "plain" })}</a3>\n`;
    x += "</doc>";
    return x;
}

// every interval attempt to post a new payload
setInterval(() => {

    // post the payload
    request.post({
        agent: agent,
        url: URL,
        body: generateXML()
    }, (error, response) => {
        if (!error && response.statusCode >= 200 && response.statusCode < 300) {
            success++;
        } else if (error) {
            fail++;
        } else {
            fail++;
        }
    });

    // update the statistics
    readline.clearLine(process.stdout, 0);
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(`success: ${success}, fail: ${fail}.`);

}, INTERVAL);
