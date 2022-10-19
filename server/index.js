const express = require("express");
const crypto = require('crypto');
var cors = require('cors');
// const { Client } = require("pg");
const https = require('https');
const fs = require('fs');
const Pool  = require('pg-pool');
const { ClientCredentialsAuthProvider } = require('@twurple/auth');
const { ApiClient } = require('@twurple/api');
const { NgrokAdapter } = require('@twurple/eventsub-ngrok');
const { DirectConnectionAdapter, EventSubListener, ReverseProxyAdapter, EventSubMiddleware } = require('@twurple/eventsub');
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

const adapter = new ReverseProxyAdapter({
  hostName: '137.184.42.175',
  port: 6970,
});

// const adapter = new DirectConnectionAdapter({
// 	hostName: '137.184.42.175',
// 	sslCert: {
// 		key: fs.readFileSync('key.pem'),
// 		cert: fs.readFileSync('cert.pem')
// 	}
// });

// const middleware = new EventSubMiddleware({
//   apiClient,
//   hostName: '164.90.246.172:6969',
//   pathPrefix: '/twitch',
//   secret: eventSubSecret
// });
// const secret = eventSubSecret;

const sender = new Sender({ bufferSize: 4096 });

const listener = new EventSubListener({ 
  apiClient, 
  adapter: adapter, 
  secret: eventSubSecret });
async function eventListener(username) {
  // console.log('adding event listeners');
  try {
  //   await listener.listen();
    await apiClient.eventSub.deleteAllSubscriptions();
  } catch { };
  const onlineSubscription = await listener.subscribeToStreamOnlineEvents(username.id, async e => {
  // await middleware.subscribeToStreamOnlineEvents(username.id, async e => {
    await sender.connect({ port: 9009, host: databaseIPV4 });
    username.live = true;
    username.startTime = (new Date()).setHours(new Date().getHours - tz);
    console.log(`${e.broadcasterDisplayName} just went live!`);
  });
  
  const offlineSubscription = await listener.subscribeToStreamOfflineEvents(username.id, async e => {
  // await middleware.subscribeToStreamOfflineEvents(username.id, e => {
    sender.close();
    username.live = false;
    username.live = null;
    username.inVodLink = false;
    username.startTime = null;
    console.log(`${e.broadcasterDisplayName} just went offline`);
  });
  username.onlineSub = onlineSubscription;
  username.offlineSub = offlineSubscription;
  // console.log(await onlineSubscription.getCliTestCommand());
};


const streamers = [
  // {name: 'MOONMOON', id: 121059319, live: null, startTime: null, onlineSub: null, offlineSub: null, inVodLink: false}, 
  {name: 'noomnoom', id: 701050844, live: null, startTime: null, onlineSub: null, offlineSub: null, inVodLink: false}, 
  
  // {name: 'A_Seagull', id: 19070311, live: null, startTime: null, onlineSub: null, offlineSub: null, inVodLink: false}
];
const chatListeners = [];
for (let i = 0; i < streamers.length; i++) {
  chatListeners.push(streamers[i].name.toLowerCase())
  eventListener(streamers[i]);
};

const app = express();
var options = { origin: 'https://moon2lights.netlify.app' };

app.use(express.json());
app.use(cors(options));
app.options('*', cors(options));
app.use(bodyParser.urlencoded({extended: false}));


const insertion = async () => {
  // console.log('adding middleware');
  // await middleware.apply(app);
  // await middleware.markAsReady();
  // for (let i = 0; i < streamers.length; i++) {
  //   chatListeners.push(streamers[i].name.toLowerCase())
  //   eventListener(streamers[i]);
  // };
  // console.log('event listeners finished adding')
  await listener.listen();
  const chatClient = new tmi.Client({
    channels: chatListeners
  });

  chatClient.connect();
  var c = 0;
  let msgTime;
  let diff;
  let vod_id;
  // console.log('about to receive message');
  chatClient.on('message', async (channel, tags, message, self) => {
    // console.log('received message!');
    roomIndex = chatListeners.indexOf(channel)
    if (streamers[roomIndex].live === null) {
      stream = await apiClient.streams.getStreamByUserId(streamers[roomIndex].id);
      try {
        if (stream.type !== 'live') {
          streamers[roomIndex].live = false;
        } else {
          streamers[roomIndex].live = true;
          try {
            await sender.connect({ port: 9009, host: databaseIPV4 });
          } catch {};
          startTime = await apiClient.videos.getVideosByUser(streamers[roomIndex].id);
          startTime = startTime.data[0].creationDate;
          startTime = startTime.setHours(startTime.getHours() - tz);
          streamers[roomIndex].startTime = startTime;
        };
      }
      catch {
        streamers[roomIndex].live = false;
      }
    };

    if (streamers[roomIndex].live) {
      msgTime = new Date();
      msgTime.setHours(new Date().getHours() - 2* tz);
      diff = (msgTime - streamers[roomIndex].startTime) / 1000;
      msgTime.setHours(~~(diff/3600));
      diff = diff % 3600;
      msgTime.setMinutes(~~(diff/60));
      diff = diff % 60;
      msgTime.setSeconds(diff);
      msgTime.setMilliseconds(0);
      msgTime = msgTime.getTime() + '000000';
      try {
        c += 1;
        sender
          .table('chatters')
          .stringColumn('username', tags['display-name'])
          .stringColumn('message', message)
          .at(msgTime);
      } catch {

      }
      }

    if (c > 10) {
      console.log('sending');
      c = 0;
      sender.reset();

      // await sender.flush();
      if (!streamers[roomIndex].inVodLink) {
        vod_id = await apiClient.videos.getVideosByUser(streamers[roomIndex].id);
        vod_id = vod_id.data[0].id;
        d = (new Date(streamers[roomIndex].startTime)).toISOString().split('T')[0];
        const vodSender = new Sender({ bufferSize: 4096});
        await vodSender.connect({ port: 9009, host: databaseIPV4 });
        vodSender
          .table('vod_link')
          .stringColumn('vid_no', vod_id)
          .stringColumn('stream_date', d)
          .atNow();
        vodSender.reset()
        // await vodSender.flush();
        vodSender.close();
        streamers[roomIndex].inVodLink = true;
      };
    };
  });
};
insertion().catch(console.error);


// "use strict"



const pool = new Pool({
  database: "chatters",
  host: databaseIPV4,
  password: "quest",
  port: 8812,
  user: "admin",
  max: 20,
})
const start = async () => {
  // app.post("/eventsub/subscriptions", (req, res) => {
  //   console.log('here22!');
  // })

  app.post("/fetch", async (req, res) => {
    const c = await pool.connect();
    const eresp = [];
    var labels = [];
    let query_res;
    for (let i=0; i < req.body.emote.length; i++) {
      emote_i = req.body.emote[i].label;
      date_i = req.body.date;
      if (emote_i === 'All Chat Messages') {
        emote_i = 'All Chat Messages'
        q = `(SELECT ts, count() c FROM 'chatters'
        WHERE ts in ${SqlString.escape(date_i)} SAMPLE BY 2m)`;
      } else {
        q = `(SELECT ts, count() c FROM 'chatters' 
        WHERE message~${SqlString.escape("(?i)^.*"+emote_i+".*$")} 
        AND ts IN ${SqlString.escape(date_i)} SAMPLE BY 2m)`;
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


  app.get("/dates", async (req, res) => {
    const c = await pool.connect();
    const uniqueDates = await c.query('SELECT DISTINCT stream_date FROM vod_link;');
    const max_res = await c.query('SELECT stream_date FROM vod_link ORDER BY stream_date DESC LIMIT 1;');
    c.release();
    res.json({dates: uniqueDates.rows, maxDate: max_res.rows});
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

}
start().catch(console.error());


https
  .createServer(
    {
      key: fs.readFileSync("key.pem"),
      cert: fs.readFileSync("cert.pem"),
    },
    app)
  .listen(PORT, () => {
    console.log(`Server listening on ${PORT}`);  
  })
// app.listen(PORT, () => {
//   console.log(`Server listening on ${PORT}`);
// });