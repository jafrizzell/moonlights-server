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

const fetch = (...args) =>
	import('node-fetch').then(({default: fetch}) => fetch(...args));
const tz = new Date().getTimezoneOffset() / 60;

const pool = new Pool({
  database: "chatters",
  host: databaseIPV4,
  password: "quest",
  port: 8812,
  user: "admin",
  max: 20,
})

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
let sender;
let vodSender;


async function liveListener(streamer) {
  let stream;
  let vods;
  streamer.lastLiveCheck = new Date();  // reset the lastLiveCheck to now
  await apiClient.streams.getStreamByUserId(streamer.id).then((s) => {stream = s});  // fetch the current stream state of the streamer
  if (stream !== null) {
    console.log(`checking...${stream.userName} is currently: ${stream.type} @ ${new Date()}`);
    if (!streamer.live) {  // if the previous status was not live and the current status is live, initiate some variables
      streamer.live = true;  // set the stream state to live
      await apiClient.videos.getVideosByUser(streamer.id).then((v) => vods = v);
      startTime = new Date(vods.data[0].creationDate);  // get the start time of the vod
      if (new Date - startTime > 90000) {  // Sometimes the twitch vod won't appear quickly
        console.log(`current (or previous) start time is ${startTime}`)
        // This causes the stream to be "live", but the code will pull the previous stream vod as the start time 
        // In this case, we assume that the stream went live 90 seconds ago
        startTime = new Date(new Date() - 90000);
        console.log(`I had to correct the start time to ${startTime} CST.`);
      }
      streamer.startTime = startTime;
      streamer.streamerLocalTime = startTime.setHours(startTime.getHours() + streamer.streamerTzOffset)
      const c = await pool.connect();
      q = `SELECT * FROM vod_link ORDER BY stream_date DESC LIMIT 1`;
      query_res = await c.query(q);
      try {
        sender = new Sender({bufferSize: 4096});
        await sender.connect({ port: 9009, host: databaseIPV4 });  // connect the database sender
      } catch { }

      vod_id = vods.data[0].id;
      d = new Date(streamer.streamerLocalTime).toISOString().split('T')[0];
      if (d === query_res.rows.stream_date) {
        q2 = `SELECT * FROM chatters ORDER BY ts DESC LIMIT 1`;
        q2_res = await c.query(q2);
        streamer.samedayOffset = q2_res.rows.ts
      }
      c.release();
      try {
        vodSender = new Sender({ bufferSize: 4096});
        await vodSender.connect({ port: 9009, host: databaseIPV4 });
      } catch {}
      vodSender
        .table('vod_link')
        .stringColumn('vid_no', vod_id)
        .stringColumn('stream_date', d)
        .atNow();
      // vodSender.reset()  // comment this for testing to prevent anything from being sent to the database
      await vodSender.flush();  // comment this for the production version
      await vodSender.close();
    }
  } else {
    console.log(`checking... ${streamer.name} is currently: not live @ ${new Date()}`);
    if (streamer.live) { // if the previous state was live and the current state is not, un-initialize some variables
      sender.close();
    }
    streamer.samedayOffset = 0
    streamer.live = false;
    streamer.startTime = null;
  }

};


const streamers = [
  {name: 'MOONMOON', id: 121059319, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null},
  // {name: 'A_Seagull', id: 19070311, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null},
  // {name: 'HisWattson', id: 123182260, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 1, samedayOffset: 0, lastLiveCheck: null},
  // {name: 'meactually', id: 92639761, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null}, 
];
const chatListeners = [];
for (let i = 0; i < streamers.length; i++) {
  chatListeners.push(streamers[i].name.toLowerCase())
};

const app = express();
var options = { origin: 'https://twitchlights.com:3000' };  // For production deployment
// var options = { origin: 'http://localhost:3000' };  // For local testing
app.use(express.json());
app.use(cors(options));
app.options('*', cors(options));
app.use(bodyParser.urlencoded({extended: false}));


const insertion = async () => {
  const chatClient = new tmi.Client({
    channels: chatListeners
  });

  await chatClient.connect();
  var c = 0;
  let msgTime;
  let diff;

  chatClient.on('message', async (channel, tags, message, self) => {
    roomIndex = chatListeners.indexOf(channel);
    // check live status every 30000 ms (30 seconds)
    if (new Date() - streamers[roomIndex].lastLiveCheck > 30000) {
      await liveListener(streamers[roomIndex]);
    };
    if (streamers[roomIndex].live) {

      if (streamers[roomIndex].startTime !== null) {
        ttime = new Date(streamers[roomIndex].startTime);
        ttime.setHours(ttime.getHours() - tz);
        msgTime = new Date();  // get the current time and set the HH:MM:SS to the stream uptime
        msgTime.setHours(msgTime.getHours() - tz);
        msgTime.setHours(msgTime.getHours() + streamers[roomIndex].streamerTzOffset);
        diff = (msgTime - ttime) / 1000;
        ttime.setHours(~~(diff/3600) - tz);
        diff = diff % 3600;
        ttime.setMinutes(~~(diff/60));
        diff = diff % 60;
        ttime.setSeconds(diff);
        ttime.setMilliseconds(000);
        ttime = new Date(ttime + streamers[roomIndex].samedayOffset);
        ttime = ttime.getTime() + '000000';
        try {  // for some reason the timestamp above can be invalid? So this is wrapped in a try/catch
          c += 1;
          sender
            .table('chatters')
            .stringColumn('username', tags['display-name'])
            .stringColumn('message', message)
            .at(ttime);
        } catch { }
        }
      }

    if (c > 10) {  // only send batches of 10 messages to the database to minimize traffic volume
      // console.log(`sending from ${channel} @`, new Date(ttime));
      c = 0;
      // sender.reset();  // comment this for testing to not send any data to the database
      await sender.flush();
    };
  });
};
insertion().catch(console.error);


// "use strict"


// const pool = new Pool({
//   database: "chatters",
//   host: databaseIPV4,
//   password: "quest",
//   port: 8812,
//   user: "admin",
//   max: 20,
// })
const start = async () => {
  app.get("/auth", (req, res) => {
    res.json('authenticated');

  });

  app.post("/fetch", async (req, res) => {
    date_i = req.body.date;
    const c = await pool.connect();
    const eresp = [];
    var labels = [];
    let query_res;
    // console.log('querying for sampling rate');
    rate = 240;  // number of buckets to sample the data into, more = finer resolution but slightly slower to load
    const sampling_q = `SELECT CAST((3600*hour(max(ts)) + 60*minute(max(ts)) + second(max(ts)))/${rate} AS string)
                        FROM 'chatters' WHERE ts IN '${date_i}';`
    const spacing = await c.query(sampling_q);
    if (spacing.rows[0].cast < 1) {
      spacing.rows[0].cast = 1;
    };
    const sampling = spacing.rows[0].cast+'s';
    for (let i=0; i < req.body.emote.length; i++) {
      emote_i = req.body.emote[i].label;
      if (emote_i === 'All Chat Messages') {
        emote_i = 'All Chat Messages'
        q = `(SELECT ts, count() c FROM 'chatters'
        WHERE ts in ${SqlString.escape(date_i)} SAMPLE BY ${sampling} FILL(LINEAR))`;
      } else {
        q = `(SELECT ts, count() c FROM 'chatters' 
        WHERE message~${SqlString.escape("(?i)^.*"+emote_i+".*$")} 
        AND ts IN ${SqlString.escape(date_i)} SAMPLE BY ${sampling} FILL(LINEAR))`;
      }
      query_res = await c.query(q);
      var colors = palette('mpn65', req.body.emote.length);
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
        d = query_res.rows[r]['ts']
        d.setHours(d.getHours());
        d.setSeconds(0);
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
    ;

  });


  app.post("/dates", async (req, res) => {
    // console.log('here');
    const c = await pool.connect();
    const uniqueDates = await c.query('SELECT DISTINCT * FROM vod_link;');
    const max_res = await c.query('SELECT stream_date FROM vod_link ORDER BY stream_date DESC LIMIT 1;');
    c.release();
    let lstream;
    let live;
    await apiClient.streams.getStreamByUserName(req.body.username).then((s) => lstream = s);
    if (lstream !== null) {
      live = true;
    } else {
      live = false;
    }
    res.json({dates: uniqueDates.rows, maxDate: max_res.rows, live: live});
    ;

});

  app.post("/topEmotes", async (req, res) => {
    const c = await pool.connect();
    q = `SELECT DISTINCT message, count() c FROM 'chatters' WHERE ts IN ${SqlString.escape(req.body.date)} ORDER BY c DESC LIMIT 100;`
    const query_res = await c.query(q);
    const topEmotes = []
    for (let i = 0; i < query_res.rows.length; i++) {
      topEmotes.push(query_res.rows[i].message)
    }
    c.release();
    res.json({topEmotes: topEmotes});
    ;
  });

  app.get("/dev", (req, res) => {
    res.json({response: "Im here!"})
  })
}
start().catch(console.error());

// un-comment this for deployment
// https
//   .createServer(
//     {
//       key: fs.readFileSync("key.pem"),
//       cert: fs.readFileSync("cert.pem"),
//     },
//     app)
//   .listen(PORT, () => {
//     console.log(`Server listening on ${PORT}`);  
//   })

// un-comment this for testing
app.listen(PORT, () => {
  console.log(`Server listening on ${PORT}`);
});