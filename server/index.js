const express = require("express");
var cors = require('cors');
const https = require('https');
const fs = require('fs');
const Pool  = require('pg-pool');
const { ClientCredentialsAuthProvider } = require('@twurple/auth');
const { ApiClient } = require('@twurple/api');
const { Sender } = require("@questdb/nodejs-client");
const bodyParser = require('body-parser');
const palette = require('./palette');
var SqlString = require('sqlstring');
const credentials = require('../secrets/secrets.js');
const PORT = process.env.PORT || 6969;
const tmi = require('tmi.js');


const TESTING = false;

const fetch = (...args) =>
	import('node-fetch').then(({default: fetch}) => fetch(...args));
const tz = new Date().getTimezoneOffset() / 60;

// const pool = new Pool({
//   database: "chatters",
//   host: databaseIPV4,
//   password: "quest",
//   port: 8812,
//   user: "admin",
//   max: 20,
// })

const Url = "https://id.twitch.tv/oauth2/token"
const Data = {
  client_id: clientId,
  client_secret: clientSecret,
  grant_type: "client_credentials"
}
const Params = {
  headers: { "Content-Type": "application/json" },
  body: Data,
  method: "POST"
}

let access_token = "";
fetch(Url, Params)
  .then(response => {access_token = response.json()["access_token"]});


const authProvider = new ClientCredentialsAuthProvider(clientId, clientSecret);
const apiClient = new ApiClient({ authProvider });


// const sender = new Sender({ bufferSize: 4096 });
// const vodSender = new Sender({ bufferSize: 4096});  // create and connect a sender for the vod_link table
// let sender;
// let vodSender;


// async function liveListener(streamer) {
//   let stream;
//   let vods;
//   streamer.lastLiveCheck = new Date();  // reset the lastLiveCheck to now
//   await apiClient.streams.getStreamByUserId(streamer.id).then((s) => {stream = s});  // fetch the current stream state of the streamer
//   if (stream !== null) {
//     // console.log(`checking...${stream.userName} is currently: ${stream.type} @ ${new Date()}`);
//     if (!streamer.live) {  // if the previous status was not live and the current status is live, initiate some variables
//       streamer.live = true;  // set the stream state to live
//       await apiClient.videos.getVideosByUser(streamer.id).then((v) => vods = v);
//       startTime = new Date(vods.data[0].creationDate);  // get the start time of the vod
//       if (new Date - startTime > 90000) {  // Sometimes the twitch vod won't appear quickly
//         // console.log(`current (or previous) start time is ${startTime}`)
//         // This causes the stream to be "live", but the code will pull the previous stream vod as the start time 
//         // In this case, we assume that the stream went live 90 seconds ago
//         startTime = new Date(new Date() - 90000);
//         // console.log(`I had to correct the start time to ${startTime} CST.`);
//       }
//       streamer.startTime = startTime;
//       streamer.streamerLocalTime = startTime.setHours(startTime.getHours() + streamer.streamerTzOffset)
//       const c = await pool.connect();
//       q = `SELECT * FROM vod_link ORDER BY stream_date DESC LIMIT 1`;
//       query_res = await c.query(q);
//       try {
//         sender = new Sender({bufferSize: 4096});
//         await sender.connect({ port: 9009, host: databaseIPV4 });  // connect the database sender
//       } catch { }

//       vod_id = vods.data[0].id;
//       d = new Date(streamer.streamerLocalTime).toISOString().split('T')[0];
//       if (d === query_res.rows.stream_date) {
//         q2 = `SELECT * FROM chatters ORDER BY ts DESC LIMIT 1`;
//         q2_res = await c.query(q2);
//         streamer.samedayOffset = q2_res.rows.ts
//       }
//       c.release();
//       try {
//         vodSender = new Sender({ bufferSize: 4096});
//         await vodSender.connect({ port: 9009, host: databaseIPV4 });
//       } catch {}
//       vodSender
//         .table('vod_link')
//         .stringColumn('vid_no', vod_id)
//         .stringColumn('stream_date', d)
//         .stringColumn('stream_name', '#'.concat(streamer.name.toLowerCase()))
//         .atNow();
//       if (TESTING) {
//         vodSender.reset();  // When testing, don't send data to the database
//       }
//       else {
//         await vodSender.flush();  // Send the data to the database
//       }
//       await vodSender.close();
//     }
//   } else {
//     // console.log(`checking... ${streamer.name} is currently: not live @ ${new Date()}`);
//     if (streamer.live) { // if the previous state was live and the current state is not, un-initialize some variables
//       sender.close();
//     }
//     streamer.samedayOffset = 0
//     streamer.live = false;
//     streamer.startTime = null;
//   }

// };


const streamers = [
  {name: 'MOONMOON', id: 121059319, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'nyanners', id: 82350088, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'PENTA', id: 84316241, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'A_Seagull', id: 19070311, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null},
  {name: 'GEEGA', id: 36973271, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null},
  
  // {name: 'HisWattson', id: 123182260, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 1, samedayOffset: 0, lastLiveCheck: null},
  // {name: 'meactually', id: 92639761, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null}, 
];
const chatListeners = [];
for (let i = 0; i < streamers.length; i++) {
  chatListeners.push(streamers[i].name.toLowerCase())
};

const app = express();
if (TESTING) {
  var options = { origin: 'http://localhost:3000' };  // For local testing
}
else {
  var options = { origin: 'https://twitchlights.com' };  // For production deployment
};
app.use(express.json());
app.use(cors(options));
app.options('*', cors(options));
app.use(bodyParser.urlencoded({extended: false}));


// const insertion = async () => {
//   const chatClient = new tmi.Client({
//     channels: chatListeners
//   });

//   await chatClient.connect();
//   var c = 0;
//   let msgTime;
//   let diff;

//   chatClient.on('message', async (channel, tags, message, self) => {
//     roomIndex = chatListeners.indexOf(channel);
//     // check live status every 30000 ms (30 seconds)
//     if (new Date() - streamers[roomIndex].lastLiveCheck > 30000) {
//       await liveListener(streamers[roomIndex]);
//     };
//     if (streamers[roomIndex].live) {

//       if (streamers[roomIndex].startTime !== null) {
//         ttime = new Date(streamers[roomIndex].startTime);
//         ttime.setHours(ttime.getHours() - tz);
//         msgTime = new Date();  // get the current time and set the HH:MM:SS to the stream uptime
//         msgTime.setHours(msgTime.getHours() - tz);
//         msgTime.setHours(msgTime.getHours() + streamers[roomIndex].streamerTzOffset);
//         diff = (msgTime - ttime) / 1000;
//         ttime.setHours(~~(diff/3600) - tz);
//         diff = diff % 3600;
//         ttime.setMinutes(~~(diff/60));
//         diff = diff % 60;
//         ttime.setSeconds(diff);
//         ttime.setMilliseconds(000);
//         ttime = new Date(ttime + streamers[roomIndex].samedayOffset);
//         ttime = ttime.getTime() + '000000';
//         try {  // for some reason the timestamp above can be invalid? So this is wrapped in a try/catch
//           c += 1;
//           sender
//             .table('chatters')
//             .symbol('stream_name', channel)
//             .stringColumn('username', tags['display-name'])
//             .stringColumn('message', message)
//             .at(ttime);
//         } catch { console.log('error encountered when sending to the chatters table')}
//         }
//       }

//     if (c > 10) {  // only send batches of 10 messages to the database to minimize traffic volume
//       c = 0;
//       if (TESTING) {
//         sender.reset();  // When testing, don't send data to the database
//       }
//       else {
//         await sender.flush();  // Send data to the database
//       }
//     };
//   });
// };
// insertion().catch(console.error);



const pool = new Pool({
  database: "chatters",
  host: databaseIPV4,
  password: "quest",
  port: 8812,
  user: "admin",
  max: 25,
})
const start = async () => {
  app.get("/auth", (req, res) => {
    res.json('authenticated');

  });

  app.post("/fetch", async (req, res) => {
    date_i = req.body.date;
    const c = await pool.connect();
    const eresp = [];
    var labels = req.body.labels;
    var validColors = req.body.openColors;
    var allcolors = palette('mpn65', 10);
    var colors = [];
    for (let i=0; i < validColors.length; i++) {
      colors.push(allcolors[validColors[i]]);
    }
    let query_res;
    rate = 240;  // number of buckets to sample the data into, more = finer resolution but slightly slower to load
    const sampling_q = `SELECT CAST((3600*hour(max(ts)) + 60*minute(max(ts)) + second(max(ts)))/${rate} AS string)
    FROM 'chatters' WHERE stream_name=${SqlString.escape(req.body.username)} AND ts IN '${date_i}';`
    const spacing = await c.query(sampling_q);
    if (spacing.rows[0].cast < 1) {
      spacing.rows[0].cast = 1;
    };
    const sampling = spacing.rows[0].cast+'s';
    for (let i=0; i < req.body.emote.length; i++) {
      emote_i = req.body.emote[i].label;
      // console.log(emote_i);
      var emote_fixed = emote_i.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      // console.log(SqlString.escape("(?i)"+emote_i));
      // console.log(SqlString.escape("(?i)"+emote_i).replace("\\\\", "\\"));
      if (emote_fixed === 'All Chat Messages') {
        emote_fixed = 'All Chat Messages'
        q = `(SELECT ts, count() c FROM 'chatters'
        WHERE ts in ${SqlString.escape(date_i)} AND stream_name=${SqlString.escape(req.body.username)} SAMPLE BY ${sampling} FILL(0))`;
      } else {
        q = `(SELECT ts, count() c FROM 'chatters' 
        WHERE message~${SqlString.escape("(?i)"+emote_fixed).replace("\\\\", "\\")} 
        AND ts IN ${SqlString.escape(date_i)} AND stream_name=${SqlString.escape(req.body.username)} SAMPLE BY ${sampling} FILL(0))`;
        // q = `SELECT ts, sum(round_up((length(message) - length(regexp_replace(message, '(?i)${emote_i}', '')))/length('${emote_i}'), 0)) c FROM 'chatters'
        //     WHERE message~${SqlString.escape("(?i)^.*"+emote_i+".*$")}
        //     AND ts IN ${SqlString.escape(date_i)} AND stream_name='${req.body.username}' SAMPLE BY ${sampling} FILL(0);`
      }
      query_res = await c.query(q);
      eresp.push(
        {
          label: emote_i, 
          type:'line', 
          data:[], 
          cubicInterpolationMode: 'monotone', 
          backgroundColor: '#'+colors[i], 
          borderColor: '#'+colors[i],
          borderWidth: 2,
          pointRadius: 2,
        }
        );
        for (let r=0; r < query_res.rows.length; r++) {
          d = query_res.rows[r]['ts'];
          // d.setHours(d.getHours());
          // d.setSeconds(0);
          function fixTime(i) {
            if (i < 10) {
              i = '0' + i;
            };
            return i;
          }
          var h = d.getHours();
          var m = d.getMinutes();
          var s = d.getSeconds();
          h = fixTime(h);
          m = fixTime(m);
          s = fixTime(s);
          var n = h+":"+m+":"+s;
          eresp[i]['data'].push({x: n, y: query_res.rows[r]['c']})
          if (!labels.includes(n)) {
            labels.push(n);
          }
        };
      };

      c.release();
      res.json({datasets:eresp, labels:labels.sort()});

  });


  app.post("/dates", async (req, res) => {
    const c = await pool.connect();
    const uniqueDates = await c.query(`SELECT * FROM vod_link WHERE stream_name = '${req.body.username}' ORDER BY stream_date DESC;`);
    c.release();
    const max_res = uniqueDates.rows.slice(0, 1);
    let lstream;
    let live;
    await apiClient.streams.getStreamByUserName(req.body.username.slice(1)).then((s) => lstream = s);
    if (lstream !== null) {
      live = true;
    } else {
      live = false;
    }
    res.json({dates: uniqueDates.rows, maxDate: max_res, live: live});
});

  app.post("/topEmotes", async (req, res) => {
    const c = await pool.connect();
    q = `SELECT DISTINCT message, count() c FROM 'chatters' WHERE stream_name = '${req.body.username}' AND ts IN ${SqlString.escape(req.body.date)} ORDER BY c DESC LIMIT 100;`
    const query_res = await c.query(q);
    const topEmotes = []
    for (let i = 0; i < query_res.rows.length; i++) {
      if (query_res.rows[i].message.split(" ").length > 1) {
        var split_msg = query_res.rows[i].message.split(" ");
        for (let j = 0; j < Math.ceil(split_msg.length / 2); j++) {
          if (split_msg.slice(0, j+1).toString() == split_msg.slice(j+1, 2*(j+1)).toString()) {
            query_res.rows[i].message = split_msg.slice(0, j+1).join(' ');
            break;
          }
        }
      }
      if (!topEmotes.includes(query_res.rows[i].message.trim()) && query_res.rows[i].message.length < 50) {
        topEmotes.push(query_res.rows[i].message);
      }
    }
    c.release();
    res.json({topEmotes: topEmotes});
    ;
  });

  app.get("/names", (req, res) => {
    // res.json({names: chatListeners,})
    res.json({streams: streamers, names: chatListeners})
  })

  app.get("/dev", (req, res) => {
    res.json({response: "Im here!"})
  })
}
start().catch(console.error());

if (TESTING) {
// Use HTTP protocol for local testing
app.listen(PORT, () => {
  console.log(`Server listening on ${PORT}`);
});
}
else {
  // Use HTTPS protocol for production
  https
  .createServer(
    {
      key: fs.readFileSync("/etc/letsencrypt/live/twitchlights.com/privkey.pem"),
      cert: fs.readFileSync("/etc/letsencrypt/live/twitchlights.com/cert.pem"),
    },
    app)
  .listen(PORT, () => {
    console.log(`Server listening on ${PORT}`);  
  })
}


