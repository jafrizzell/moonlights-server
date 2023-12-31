const Pool  = require('pg-pool');
const { ClientCredentialsAuthProvider } = require('@twurple/auth');
const { ApiClient } = require('@twurple/api');
const { Sender } = require("@questdb/nodejs-client");
const credentials = require('../secrets/secrets.js');
const PORT = process.env.PORT || 6969;
const tmi = require('tmi.js');


const TESTING = false;

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


let sender;
let vodSender;


async function liveListener(streamer) {
  let stream;
  let vods;
  streamer.lastLiveCheck = new Date();  // reset the lastLiveCheck to now
  try{ await apiClient.streams.getStreamByUserId(streamer.id).then((s) => {stream = s}); }  // fetch the current stream state of the streamer
  catch {console.log('api call failed')};
  if (stream !== null) {
    if (streamer.live != 'true') {  // if the previous status was not live and the current status is live, initiate some variables
      streamer.live = 'true';  // set the stream state to live
      await apiClient.videos.getVideosByUser(streamer.id).then((v) => vods = v);
      try {
        startTime = new Date(vods.data[0].creationDate);  // get the start time of the vod
      } catch { startTime = new Date()}
      const c = await pool.connect();
      q = `SELECT * FROM vod_link WHERE stream_name='#${streamer.name.toLowerCase()}' ORDER BY stream_date DESC LIMIT 1`;
      query_res = await c.query(q);

      if (query_res.rows.length > 0 && vods.data.length > 0) {
        if (query_res.rows[0].vid_no == vods.data[0].id) {  
          // Sometimes the twitch vod won't appear quickly
          // This causes the stream to be "live", but the code will pull the previous stream vod_id 
          
          // To solve this, we define a new state "pending"
          // In theory, this will allow us to add messages to the database for the live stream,
          // and wait for the proper vod_id to appear on twitch to add into the database.
          // To allow messages into the database, we assume the start time was 12 seconds ago
          streamer.live = 'pending';
  
          startTime = new Date();
        }
      }

      try {
        sender = new Sender({bufferSize: 4096});
        await sender.connect({ port: 9009, host: databaseIPV4 });  // connect the database sender
      } catch { }
      streamer.startTime = startTime;
      streamer.streamerLocalTime = startTime.setHours(startTime.getHours() + streamer.streamerTzOffset - tz)
      
      try {
        vod_id = vods.data[0].id;
      } catch {vod_id = '0000000000'}
      d = new Date(streamer.streamerLocalTime).toISOString().split('T')[0];
      if (query_res.rows.length > 0) {
        if (d === query_res.rows[0].stream_date) {
          q2 = `SELECT * FROM chatters WHERE stream_name='#${streamer.name.toLowerCase()}' ORDER BY ts DESC LIMIT 1`;
          q2_res = await c.query(q2);
          streamer.samedayOffset = q2_res.rows[0].ts
        }
      }
      c.release();
      if (streamer.live === 'true') {
        try {
          vodSender = new Sender({ bufferSize: 4096});
          await vodSender.connect({ port: 9009, host: databaseIPV4 });
        } catch {}
        vodSender
          .table('vod_link')
          .stringColumn('vid_no', vod_id)
          .stringColumn('stream_date', d)
          .stringColumn('stream_name', '#'.concat(streamer.name.toLowerCase()))
          .stringColumn('highlight_status', 'live')
          .atNow();
        if (TESTING) {
          vodSender.reset();  // When testing, don't send data to the database
        } else {
          await vodSender.flush();  // Send the data to the database
        }
      }
      try {
        await vodSender.close();
      } catch {}
    }
  } else {
    if (streamer.live == 'true') { // if the previous state was live and the current state is not, un-initialize some variables
      const p = await pool.connect();
      // Set the stream end time
      const endTime = new Date(new Date().setHours(new Date().getHours() - 6)).toISOString()
      endParams = [String(endTime), String(vod_id)]
      updateHighlightStatus = await p.query(`UPDATE vod_link SET highlight_status = $1 WHERE vid_no = $2 ;`, endParams);
      await p.query('COMMIT');
      p.release();
      sender.close();
    }
    streamer.samedayOffset = 0
    streamer.live = 'false';
    streamer.startTime = null;
  }

};


const streamers = [
  {name: 'MOONMOON', id: 121059319, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  // {name: 'nyanners', id: 82350088, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60}, 
  // {name: 'A_Seagull', id: 19070311, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  // {name: 'GEEGA', id: 36973271, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60}, 
  // {name: 'DougDoug', id: 31507411, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: -2, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  
  // {name: 'PENTA', id: 84316241, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null, vod_life: 60},
  // {name: 'HisWattson', id: 123182260, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 1, samedayOffset: 0, lastLiveCheck: null},
  // {name: 'meactually', id: 92639761, live: false, startTime: null, streamerLocalTime: null, streamerTzOffset: 0, samedayOffset: 0, lastLiveCheck: null, vod_life: 14}, 
  ];
  const chatListeners = [];
  for (let i = 0; i < streamers.length; i++) {
    chatListeners.push(streamers[i].name.toLowerCase())
  };

const insertion = async () => {
    const chatClient = new tmi.Client({
      channels: chatListeners
    });
  
    await chatClient.connect();
    var c = 0;
    let msgTime;
    let diff;
    
    chatClient.on('message', async (channel, tags, message, self) => {
      if (self) return;
      const roomIndex = chatListeners.indexOf(channel);
      // check live status every 5000 ms (5 seconds)
      if (new Date() - streamers[roomIndex].lastLiveCheck > 5000) {
        await liveListener(streamers[roomIndex]);
        
      };
      if (streamers[roomIndex].live != 'false') {
        if (streamers[roomIndex].startTime !== null) {
          ttime = new Date(streamers[roomIndex].startTime);
          ttime.setHours(ttime.getHours() - tz);
          msgTime = new Date();  // get the current time and set the HH:MM:SS to the stream uptime
          msgTime.setHours(msgTime.getHours() - tz);
          msgTime.setHours(msgTime.getHours() + streamers[roomIndex].streamerTzOffset);
          diff = (msgTime - ttime) / 1000;
          ttime.setHours(~~(diff/3600) - tz)
          try {
            ttime.setHours(ttime.getHours() + streamers[roomIndex].samedayOffset.getHours());
          } catch {}
          diff = diff % 3600;
          ttime.setMinutes(~~(diff/60));
          try {
            ttime.setMinutes(ttime.getMinutes() + streamers[roomIndex].samedayOffset.getMinutes());
          } catch {}
          diff = diff % 60;
          ttime.setSeconds(diff);
          try {
            ttime.setSeconds(ttime.getSeconds() + streamers[roomIndex].samedayOffset.getSeconds());
          } catch {}
          ttime.setMilliseconds(000);
          
          ttime = ttime.getTime() + '000000';
          try {  // for some reason the timestamp above can be invalid? So this is wrapped in a try/catch
            c += 1;
            sender
              .table('chatters')
              .symbol('stream_name', channel)
              .stringColumn('username', tags['display-name'])
              .stringColumn('message', message)
              .at(ttime);
          } catch { console.log('error encountered when sending to the chatters table')}
          }
        }
  
      if (c > 10) {  // only send batches of 10 messages to the database to minimize traffic volume
        c = 0;
        if (TESTING) {
          sender.reset();  // When testing, don't send data to the database
        }
        else {
          try {
            await sender.flush();  // Send data to the database
            } catch {
              sender = new Sender({bufferSize: 4096});
              await sender.connect({ port: 9009, host: databaseIPV4 });  // connect the database sender
              await sender.flush();
             }
        }
      };
    });
  };
  insertion().catch(console.error);