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
// var SqlString = require('sqlstring');
const credentials = require('../secrets/secrets.js');
const PORT = process.env.PORT || 6969;
const tmi = require('tmi.js');


const TESTING = false;

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

const streamers = [
  {name: 'MOONMOON', id: 121059319, accentColor: '#54538C', textColor: '#EAEEF2', live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'nyanners', id: 82350088, accentColor: '#ebbfce', textColor: '#000000', live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'A_Seagull', id: 19070311, accentColor: '#5bdde1', textColor: '#000000', live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  {name: 'GEEGA', id: 36973271, accentColor: '#a4b6c4',  textColor: '#000000', live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  
  // {name: 'DougDougW', id: 31507411, accentColor: '#d64a0d', textColor: '#EAEEF2', live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  // {name: 'PENTA', id: 84316241, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
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
    const rate = req.body.spacing;  // number of buckets to sample the data into, more = finer resolution but slightly slower to load
    const sampling_q = `SELECT CAST((3600*hour(max(ts)) + 60*minute(max(ts)) + second(max(ts)))/$1 AS string)
    FROM 'chatters' WHERE stream_name=$2 AND ts IN $3;`
    sampling_q_params = [rate, req.body.username, date_i]
    const spacing = await c.query(sampling_q, sampling_q_params);
    if (spacing.rows[0].cast < 1) {
      spacing.rows[0].cast = 1;
    };
    const sampling = spacing.rows[0].cast + 's';
    for (let i=0; i < req.body.emote.length; i++) {
      emote_i = req.body.emote[i].label;
      var emote_fixed = emote_i;

      if (emote_fixed === 'All Chat Messages' || emote_i === '.*') {
        emote_fixed = 'All Chat Messages'
        q_param = [date_i, req.body.username]
        q = `SELECT ts, count() c FROM chatters
        WHERE ts in $1 AND stream_name=$2 SAMPLE BY ${sampling} FILL(0)`;
      } else {
        q_param = [emote_fixed, date_i, req.body.username]
        q = `SELECT ts, count() c FROM chatters
        WHERE message~concat('(?i\)\\b', $1, '\\b') AND ts IN $2 AND stream_name = $3 SAMPLE BY ${sampling} FILL(0)`;
        // q = `SELECT ts, sum(round_up((length(message) - length(regexp_replace(message, '(?i)${emote_i}', '')))/length('${emote_i}'), 0)) c FROM 'chatters'
        //     WHERE message~${SqlString.escape("(?i)^.*"+emote_i+".*$")}
        //     AND ts IN ${SqlString.escape(date_i)} AND stream_name='${req.body.username}' SAMPLE BY ${sampling} FILL(0);`
      }

      const query_res = await c.query(q, q_param)
 
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
    const d_params = [req.body.username]
    const uniqueDates = await c.query(`SELECT * FROM vod_link WHERE stream_name = $1 ORDER BY stream_date DESC`, d_params);
    c.release();
    const max_res = uniqueDates.rows.slice(0, 1);

    let live;
    await apiClient.streams.getStreamByUserName(req.body.username.slice(1)).then((s) => lstream = s);
    lstream !== null ? live = true : live = false;
    res.json({
      dates: uniqueDates.rows, 
      maxDate: max_res, 
      live: live,
      accentColor: streamers[chatListeners.indexOf(req.body.username.slice(1))].accentColor,
      textColor: streamers[chatListeners.indexOf(req.body.username.slice(1))].textColor
    });
});

  app.post("/topEmotes", async (req, res) => {
    const c = await pool.connect();
    const params = [req.body.username, req.body.date]
    q = `SELECT DISTINCT message, count() c FROM 'chatters' WHERE stream_name = $1 AND ts IN $2 ORDER BY c DESC LIMIT 100`
    const query_res = await c.query(q, params);
    q2 = `SELECT count() FROM 'chatters' WHERE stream_name = $1 AND ts IN $2`
    const numMsg = await c.query(q2, params)
    q3 = `SELECT count_distinct(username) FROM 'chatters' WHERE stream_name=$1 AND ts IN $2`
    const numChatters = await c.query(q3, params);
    highlight_q = `SELECT * FROM 'highlights' WHERE stream_name=$1 AND timestamp IN $2 ORDER BY score DESC;`
    const highlights = await c.query(highlight_q, params);
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
    for (let h = 0; h < highlights.rows.length; h++) {
      highlights.rows[h].timestamp.setHours(highlights.rows[h].timestamp.getHours() - 6);
    }
    c.release();

    res.json({
      topEmotes: topEmotes,
      numMsg: numMsg.rows[0].count,
      numChatters: numChatters.rows[0].count_distinct,
      highlights: highlights.rows,

    });
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


