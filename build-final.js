const fs=require('fs');const N=x=>Number(x)||0;
const disc=require('./discovered-out.json').map(r=>({...r,src:'new discovery',niche:r.niche||('query: '+r.q)}));
const pool=require('./pool-out.json').map(r=>({...r,src:'existing DB'}));
const em=require('./emails-map.json');
const seen=new Set();const all=[];
[...pool,...disc].forEach(r=>{if(r.id&&!seen.has(r.id)){seen.add(r.id);all.push(r)}});
const RED=/politic|conservativ|liberal|trump|biden|patriot|prepper|survival|homestead|gun |firearm|sermon|gospel|jesus|christ|bible|pastor|church|ministry|quran|islam|maharaj|crypto|forex|trading|weight loss|keto|carnivore/i;
const rows=all.map(r=>{
  const e=em[r.id]||{};
  const email=e.email||r.storedEmail||'';
  const tier=(r.med_views>=20000&&r.med_like>=.08&&r.med_cmt>=.01)?'A · full spec'
    :(r.med_views>=20000&&r.med_like>=.08)?'B · 20k+ & 8% like'
    :(r.med_views>=20000&&r.med_like>=.06&&r.med_cmt>=.006)?'C · near miss'
    :(r.med_views>=20000&&r.med_like>=.05)?'D · 20k+ & 5% like':'E · below bar';
  return {tier,email,src:r.src,handle:r.handle,title:r.title,id:r.id,
    flag:RED.test([r.handle,r.title,r.niche].join(' '))?'REVIEW':'',
    niche:r.niche||'',country:e.country||r.country||'',subs:r.subs,
    mv:r.med_views,ml:(r.med_like*100).toFixed(2)+'%',mc:(r.med_cmt*100).toFixed(2)+'%',
    shorts:r.shortPct+'%',n:r.n,
    botl:(r.bot_like*100).toFixed(2)+'%',botc:(r.bot_cmt*100).toFixed(2)+'%'};
});
const ord=t=>['A','B','C','D','E'].indexOf(t[0]);
rows.sort((a,b)=>ord(a.tier)-ord(b.tier)||(b.email?0:1)-(a.email?0:1)||N(b.mv)-N(a.mv));
const esc=v=>'"'+String(v??'').replace(/"/g,'""')+'"';
const HD=['Rank','Tier','Email','Fit Flag','Handle','Channel Title','Channel URL','Source','Niche / Found Via','Country','Subscribers','Median Views','Median Like %','Median Comment %','% Shorts','Videos Sampled','Bot Like % (old)','Bot Comment % (old)'];
fs.writeFileSync('C:/Users/josen/OneDrive/Escritorio/Claude Test/bemellou-creators-verified.csv',
 [HD.map(esc).join(','),...rows.map((r,i)=>[i+1,r.tier,r.email,r.flag,r.handle,r.title,
  'https://youtube.com/'+(r.handle||'channel/'+r.id),r.src,r.niche,r.country,r.subs,r.mv,r.ml,r.mc,r.shorts,r.n,r.botl,r.botc].map(esc).join(','))].join('\n'));
const T={};rows.forEach(r=>T[r.tier]=(T[r.tier]||0)+1);
const E={};rows.forEach(r=>{if(r.email)E[r.tier]=(E[r.tier]||0)+1});
console.log('total: '+rows.length+' channels, all metrics recomputed per-video\n');
console.log('tier'.padEnd(22)+'count'.padStart(6)+'  with email');
Object.keys(T).sort().forEach(k=>console.log(k.padEnd(22)+String(T[k]).padStart(6)+String(E[k]||0).padStart(12)));
console.log('\ntotal emails: '+rows.filter(r=>r.email).length);
console.log('\nTIER A + B with an email (your realistic send list): '+rows.filter(r=>/^[AB]/.test(r.tier)&&r.email).length);
