require('dotenv').config();
const fs=require('fs');const {yt}=require('./recompute-median.js');
const all=[...require('./discovered-out.json'),...require('./pool-out.json')];
const byId={};all.forEach(r=>{if(r.id&&!byId[r.id])byId[r.id]=r});
const ids=Object.keys(byId);
console.error('fetching descriptions for '+ids.length+' channels in '+Math.ceil(ids.length/50)+' calls');
// wider extraction: plain, (at)/[at], " at ", dot-obfuscation
function extract(t){
  if(!t)return null;
  let s=t.replace(/\s*[\(\[\{]\s*(at|@)\s*[\)\]\}]\s*/gi,'@')
        .replace(/\s+at\s+(?=[a-z0-9.-]+\s*(\.|\s*[\(\[\{]?\s*dot\s*[\)\]\}]?\s*)\s*(com|net|org|co|io|uk|de|se))/gi,'@')
        .replace(/\s*[\(\[\{]?\s*(dot)\s*[\)\]\}]?\s*/gi,'.');
  const m=s.match(/\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b/g)||[];
  const ok=m.filter(e=>!/example\.com|youtu|google|sentry|email@|\.png|\.jpg/i.test(e)&&e.length<80);
  return ok[0]||null;
}
(async()=>{
  const out={};
  for(let i=0;i<ids.length;i+=50){
    const chunk=ids.slice(i,i+50);
    try{
      const r=await yt('channels',{part:'snippet,brandingSettings',id:chunk.join(','),maxResults:50});
      (r?.items||[]).forEach(c=>{
        const d1=c.snippet?.description||'',d2=c.brandingSettings?.channel?.description||'';
        out[c.id]={email:extract(d1)||extract(d2),country:c.snippet?.country||''};
      });
    }catch(e){console.error('  chunk failed: '+e.message);if(/EXHAUSTED/.test(e.message))break;}
    if((i/50)%10===0)console.error('  '+i+'/'+ids.length);
  }
  fs.writeFileSync('emails-map.json',JSON.stringify(out,null,1));
  const n=Object.values(out).filter(x=>x.email).length;
  console.error('done. channels with email in bio: '+n+' of '+Object.keys(out).length);
})();
