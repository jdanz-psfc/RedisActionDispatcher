
const host = 'localhost';
const port = 8123;
var expName = 'ACTION';
var shot = 1;
const ACTION_NOT_DISPATCHED = 0;
const ACTION_DOING = 1;
const ACTION_DONE = 2;
const ACTION_ERROR = 3;
const ACTION_TIMEOUT = 4;
const ACTION_ABORT = 5;
const ACTION_STREAMING= 6;

const page = '<!DOCTYPE html> \
<html> \
<style> \
table, th, td {\
  border:0px solid black;\
}\
th {\
    text-align: left;\
  }\
</style>\
<body>\
\
<table style="width:100%">\
  <tr>\
    <td style="color:Red">Emil</td>\
    <td>Tobias</td>\
    <td>Linus</td>\
  </tr>\
  <tr>\
    <td>16</td>\
    <td>14</td>\
    <td>10</td>\
  </tr>\
</table>'
/*
const requestListener = function(req, res){
    res.setHeader("Content-Type", "text/html");
    res.writeHead(200);
    res.end(page);
};

const server = http.createServer(requestListener);

server.listen(port, host, () => {
    console.log(`Server is running on http://${host}:${port}`);
});

*/
var sendInterval = 500;
 

const http = require('http');
const fs = require('fs');
const redis = require('redis');
const client = redis.createClient({host:'localhost'});
client.on("error", function(err) {console.log("Error: " + err);});
client.on("connect", function() {console.log("Connected to Redis"); startWebServer();});
client.connect();

/* async function getActionServers()
{
    keys = await client.keys(expName+':*');
    servers = []
    for (var i = 0; i < keys.length; i++)
    {
        fields = keys[i].split(':')
        if (!servers.includes(fields[3]))
            servers.push(fields[3]);
    }
    return servers;
} */

async function getActionServers()
{
    keys = await client.hKeys('ActionServers')
    servers = []
    for (var i = 0; i < keys.length; i++)
    {
        servers.push(keys[i]);
    }
    return servers
}

async function getPhases()
{
    console.log("SONO LA GET PHASE");
    phases = await client.sMembers(expName+':'+shot+':Phases');
    console.log("LETTE FASI");
    return  phases;
}

async function buildPhaseButtons()
{
    buttonsHtml = ""
    console.log('ORA CHIAMO BUUOILD');
    phases = await getPhases();
    console.log('ORA CHIAMATO BUUOILD');
    console.log('Phases: ');
    console.log(phases);
    for (var i = 0; i < phases.length; i++)
        buttonsHtml += '<button onclick = "doPhase(\''+phases[i]+'\')">'+phases[i]+'</button>';
    console.log(buttonsHtml);
    return buttonsHtml; 
}
async function buildDispatchTables(inExpName, inShot)
{
    expName = inExpName.toUpperCase();
    shot = Number.parseInt(inShot);
    var servers = await getActionServers();
    for(var i = 0; i < servers.length; i++)
    {
        await client.publish('COMMAND:'+servers[i], 'BUILD_TABLES:'+expName+':'+shot)
    }
}

async function doPhase(phase)
{
    var servers = await getActionServers();
    for(var i = 0; i < servers.length; i++)
    {
        await client.publish('COMMAND:'+servers[i], 'DO_PHASE:'+phase)
    }
}


async function buildActionsTable()
{
    var servers = await getActionServers();
    var table = '<table id = "Actions" style="width:100%">\
    <tr>\
    <th>Action</th>\
    <th>Phase</th>\
    <th>Server</th>\
    <th>Status</th></tr>'

    for(var i = 0; i < servers.length; i++)
    {
        key = expName+':'+shot+':ActionPathStatus:'+servers[i];
        keyNid = expName+':'+shot+':ActionInfo:'+servers[i];
        keyPhase = expName+':'+shot+':ActionPhaseInfo:'+servers[i];
        actions = await client.hGetAll(key);
        actKeys = await client.hKeys(key);
        for (var r =0; r < actKeys.length; r++)
        {
            path = actKeys[r];
            phase =  await client.hGet(keyPhase, path);
            stat = Number.parseInt(actions[actKeys[r]]);
            var color;
            var statname;
            var targetNid = '';
            switch(stat) {
                case ACTION_NOT_DISPATCHED:
                    color = 'Grey';
                    statname = 'Not Dispatched';
                    break;
                case ACTION_DOING:
                    color = 'Blue';
                    statname = 'Doing';
                    targetNid = await client.hGet(keyNid, path);
                    break;
                case ACTION_DONE:
                    color = 'Green';
                    statname = 'Done';
                   break;
                case ACTION_ERROR:
                    color = 'Red';
                    statname = 'Error';
                    break;;
                case ACTION_TIMEOUT:
                    color = 'Red';
                    statname = 'Timeout';
                    break;
                case ACTION_ABORT:
                    color = 'Red';
                    statname = 'Aborted';
                   break;
                case ACTION_STREAMING:
                    color = 'Blue';
                    statname = 'Streaming';
                    break;
                default:
                    table += '<tr>';
                
            }
            if(targetNid == '')
                table += '<tr style="color:'+color+'">';
            else
                table += '<tr style="color:'+color+'" onclick = doAbort("'+targetNid+':'+servers[i]+'")>';
               
            if (stat == ACTION_STREAMING)
                table += '<td><b>'+path+'</b></td><td>'+phase+'</td><td>'+servers[i]+'</td><td><blink>'+statname+'</blink></td></tr>'
            else
                table += '<td><b>'+path+'</b></td><td>'+phase+'</td><td>'+servers[i]+'</td><td>'+statname+'</td></tr>'
        }        
    }
    table += '</table>';
    return table;
}

async function doAbort(nidStr, server)
{
    key = expName+':'+shot+':AbortRequest:'+server;
    await client.hSet(key, nidStr, '1');
}


/*
 * send interval in millis
 */
var prevTable = '';

function sendServerSendEvent(req, res) {
    res.writeHead(200, {
        'Content-Type' : 'text/event-stream',
        'Cache-Control' : 'no-cache',
        'Connection' : 'keep-alive'
    });
 
    var sseId = (new Date()).toLocaleTimeString();
    
    writeServerSendEvent(res);
   
    setInterval(function() {
        writeServerSendEvent(res);
    }, sendInterval);
    
}
async function writeServerSendEvent(res) {
   // res.write('id: ' + sseId + '\n');
 //  res.write("data: new server event " + data + '\n\n');
   var table = await buildActionsTable();
  // if (table != prevTable)
   {
        prevTable = table;
        res.write("data: " + table + '\n\n');
    } 
}
 

async function startWebServer()
{

    http.createServer(async function(req, res) {
        console.log(req.url);
        if (req.headers.accept && req.headers.accept == 'text/event-stream') {
            if (req.url == '/talk') {
                prevTable = '';
                sendServerSendEvent(req, res);
            } else {
                res.writeHead(404);
                res.end();
            }
        } else {
            if (req.url.substring(0,6) == '/abort') {
                fields = req.url.split(':');
                doAbort(fields[1], fields[2]);
                console.log('ABORT '+ fields[1]+ '  '+ fields[2]);
            }
            else if(req.url.substring(0,12) == '/BuildTables')
            {
                fields = req.url.split(':');
                buildDispatchTables(fields[1], fields[2])
                res.writeHead(200, {
                    'Content-Type' : 'text/html'
                });
                buttonsHtml = await buildPhaseButtons()
                res.write(buttonsHtml);
                res.end();
            }
            else if (req.url.substring(0,8) == '/DoPhase') {
                fields = req.url.split(':');
                console.log('Do Phase '+ fields[1]);
                fields = req.url.split(':');
                doPhase(fields[1]);
                res.writeHead(200, {
                    'Content-Type' : 'text/html'
                });
                res.write("Ok");
                res.end();
             }
            else
            {
                res.writeHead(200, {
                    'Content-Type' : 'text/html'
                });
                res.write(fs.readFileSync(__dirname + '/index.html'));
                res.end();
            }
        }
    }).listen(8080);
}
