require('dotenv').config();
const fs=require('fs');
const {analyze,yt}=require('./recompute-median.js');
// Bemellou-fit queries skewed to high-engagement, emotionally-honest, Shorts-heavy content
const Q=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const seen=new Set(JSON.parse(fs.readFileSync(process.argv[4]||'seen.json','utf8').toString()||'[]'));
(async()=>{
  const chans=new Map();
  for(const q of Q){
    try{
      const r=await yt('search',{part:'snippet',type:'video',q,maxResults:50,order:'relevance',
        relevanceLanguage:'en',publishedAfter:new Date(Date.now()-300*864e5).toISOString()});
      (r?.items||[]).forEach(it=>{
        const cid=it.snippet?.channelId;if(!cid)return;
        if(!chans.has(cid))chans.set(cid,{id:cid,title:it.snippet?.channelTitle,q});
      });
      console.error('  "'+q+'" -> '+(r?.items?.length||0)+' vids, '+chans.size+' uniq channels');
    }catch(e){console.error('  search failed: '+e.message);if(/EXHAUSTED/.test(e.message))break;}
  }
  const fresh=[...chans.values()].filter(c=>!seen.has(c.id));
  console.error('\nunique channels: '+chans.size+' | not already in DB: '+fresh.length);
  fs.writeFileSync('discovered-raw.json',JSON.stringify(fresh,null,1));
  // analyze
  const out=[];let i=0,n=0;
  async function w(){while(i<fresh.length){const c=fresh[i++];
    try{const r=await analyze(c.id);if(r)out.push({...c,...r});}
    catch(e){if(/EXHAUSTED/.test(e.message)){i=fresh.length;console.error('QUOTA EXHAUSTED')}}
    if(++n%50===0)console.error('  analyzed '+n+'/'+fresh.length);}}
  await Promise.all(Array.from({length:5},w));
  fs.writeFileSync(process.argv[3],JSON.stringify(out,null,1));
  console.error('analyzed '+out.length+' -> '+process.argv[3]);
})();
