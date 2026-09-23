require('dotenv').config();
const axios=require('axios');const fs=require('fs');
const YT='https://www.googleapis.com/youtube/v3';
const keys=[];for(let i=1;i<=10;i++){const k=process.env[`YOUTUBE_API_KEY_${i}`];if(k&&k.trim())keys.push(k.trim());}
if(process.env.YOUTUBE_API_KEY&&!keys.includes(process.env.YOUTUBE_API_KEY.trim()))keys.push(process.env.YOUTUBE_API_KEY.trim());
if(!keys.length){console.error('no keys');process.exit(1)}
console.error('keys loaded: '+keys.length);
let ki=0;const dead=new Set();
async function yt(ep,params){
  for(let a=0;a<keys.length*2;a++){
    while(dead.has(ki)&&dead.size<keys.length)ki=(ki+1)%keys.length;
    if(dead.size>=keys.length)throw new Error('ALL_KEYS_EXHAUSTED');
    try{const r=await axios.get(`${YT}/${ep}`,{params:{...params,key:keys[ki]},timeout:20000});return r.data}
    catch(e){const s=e.response?.status;
      if(s===403||s===429){dead.add(ki);ki=(ki+1)%keys.length;continue}
      if(s===400||s===404)return null;
      await new Promise(r=>setTimeout(r,500));}
  }
  return null;
}
const med=a=>{if(!a.length)return 0;const s=[...a].sort((x,y)=>x-y);const m=s.length>>1;return s.length%2?s[m]:(s[m-1]+s[m])/2};

// per-channel: resolve -> last N uploads -> per-video ratios -> medians
async function analyze(input,N=20){
  let id=null;
  const cm=(input||'').match(/channel\/(UC[\w-]+)/i);
  if(cm)id=cm[1]; else if(/^UC[\w-]{20,}$/.test(input))id=input;
  let ch;
  if(id){ch=(await yt('channels',{part:'snippet,statistics,contentDetails',id}))?.items?.[0];}
  else{const h=(input||'').replace(/.*youtube\.com\//i,'').replace(/^@/,'').replace(/\/$/,'');
    ch=(await yt('channels',{part:'snippet,statistics,contentDetails',forHandle:h}))?.items?.[0]
     ||(await yt('channels',{part:'snippet,statistics,contentDetails',forUsername:h}))?.items?.[0];}
  if(!ch)return null;
  const up=ch.contentDetails?.relatedPlaylists?.uploads;if(!up)return null;
  const pl=await yt('playlistItems',{part:'contentDetails',playlistId:up,maxResults:N});
  const ids=(pl?.items||[]).map(i=>i.contentDetails?.videoId).filter(Boolean);
  if(ids.length<5)return null;
  const vd=await yt('videos',{part:'statistics,contentDetails',id:ids.join(',')});
  const vids=(vd?.items||[]).map(v=>{
    const vw=+(v.statistics?.viewCount||0),lk=+(v.statistics?.likeCount||0),cm=+(v.statistics?.commentCount||0);
    const dur=v.contentDetails?.duration||'';
    const m=dur.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
    const secs=m?((+m[1]||0)*3600+(+m[2]||0)*60+(+m[3]||0)):0;
    return {vw,lk,cm,secs,short:secs>0&&secs<=180};
  }).filter(v=>v.vw>0);
  if(vids.length<5)return null;
  const lr=vids.map(v=>v.lk/v.vw),cr=vids.map(v=>v.cm/v.vw),vv=vids.map(v=>v.vw);
  return {
    handle:ch.snippet?.customUrl||('channel/'+ch.id),title:ch.snippet?.title,id:ch.id,
    country:ch.snippet?.country||'',subs:+(ch.statistics?.subscriberCount||0),
    n:vids.length,shortPct:Math.round(100*vids.filter(v=>v.short).length/vids.length),
    med_views:Math.round(med(vv)),mean_views:Math.round(vv.reduce((a,b)=>a+b,0)/vv.length),
    med_like:+med(lr).toFixed(4),med_cmt:+med(cr).toFixed(4),
    // the bot's formula, for comparison
    bot_like:+(vids.reduce((s,v)=>s+v.lk,0)/vids.reduce((s,v)=>s+v.vw,0)).toFixed(4),
    bot_cmt:+(vids.reduce((s,v)=>s+v.cm,0)/vids.reduce((s,v)=>s+v.vw,0)).toFixed(4),
  };
}
module.exports={analyze,yt,med};
if(require.main===module){
  (async()=>{
    const list=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
    const out=[];let i=0,n=0;
    async function w(){while(i<list.length){const t=list[i++];
      try{const r=await analyze(t.url||t.handle);if(r)out.push({...t,...r});}catch(e){if(/EXHAUSTED/.test(e.message)){i=list.length;console.error('QUOTA EXHAUSTED')}}
      if(++n%25===0)console.error('  '+n+'/'+list.length);}}
    await Promise.all(Array.from({length:5},w));
    fs.writeFileSync(process.argv[3],JSON.stringify(out,null,1));
    console.error('analyzed '+out.length+' -> '+process.argv[3]);
  })();
}
