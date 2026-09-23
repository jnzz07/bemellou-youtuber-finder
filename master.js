const fs=require('fs');const N=x=>Number(x)||0;
const SP=require('path').join(__dirname,'data','legacy')+'/'; // snapshot rescued from a dead session Temp folder (gitignored, holds emails)
const db=require(SP+'all.json');
const emap=require('./emails-map.json');                       // fresh bio emails, wider regex
const med={};[...require('./pool-out.json'),...require('./discovered-out.json')].forEach(r=>{
  med[(r.handle||'').toLowerCase()]=r; if(r.id)med['id:'+r.id]=r;});
const liveE={};[...require(SP+'recovered.json'),...require(SP+'recovered2.json')].forEach(r=>{
  if(r.handle)liveE[r.handle.toLowerCase()]=r;});
const FIT=new Set(['mental health','neurodivergent','kawaii/plush','emotional healing','self care','chronic illness','introvert lifestyle','introvert','cozy lifestyle','asmr','journaling','books & journaling','aesthetic niche','student life','crafts & art','books','spiritual','gentle fitness','aesthetic','food & cozy','aesthetic vlog','anime','productivity']);
const RED=/politic|conservativ|liberal|trump|biden|patriot|prepper|survival|homestead|gun |firearm|sermon|gospel|jesus|christ|bible|pastor|church|ministry|quran|islam|maharaj|crypto|forex|trading|weight loss|keto|carnivore/i;

const rows=[];const seenEmail=new Set();
function add(o){
  const e=(o.email||'').trim().toLowerCase();
  if(!e||seenEmail.has(e))return; seenEmail.add(e); rows.push(o);
}
// 1) every DB creator with an email
db.forEach(r=>{
  const h=(r.handle||'').toLowerCase();
  const m=med[h]||{};
  const live=liveE[h]||{};
  const fresh=emap[m.id]?.email;
  const email=fresh||live.newEmail||((r.email&&r.email!=='Not listed')?r.email:'');
  if(!email)return;
  add({email,handle:r.handle,name:r.first_name,url:r.channel_url,niche:r.niche,
    fit:FIT.has(r.niche)?'yes':'generic/off-niche',
    flag:RED.test([r.handle,r.first_name,r.praise,r.vibe].join(' '))?'REVIEW':'',
    subs:m.subs||r.subscriber_count,
    mv:m.med_views??'',ml:m.med_like!=null?(m.med_like*100).toFixed(2)+'%':'',
    mc:m.med_cmt!=null?(m.med_cmt*100).toFixed(2)+'%':'',shorts:m.shortPct!=null?m.shortPct+'%':'',
    sv:r.avg_views,sl:(N(r.like_ratio)*100).toFixed(2)+'%',sc:(N(r.comment_ratio)*100).toFixed(2)+'%',
    ver:fresh?'verified live':(live.ok?'not in bio today':'stored only'),
    src:'finder DB',batch:r.batch_number,found:r.date_found});
});
// 2) newly discovered channels with an email (not already in DB)
require('./discovered-out.json').forEach(r=>{
  const e=emap[r.id]?.email; if(!e)return;
  add({email:e,handle:r.handle,name:r.title,url:'https://youtube.com/'+(r.handle||'channel/'+r.id),
    niche:'found via: '+r.q,fit:'unclassified',
    flag:RED.test([r.handle,r.title,r.q].join(' '))?'REVIEW':'',subs:r.subs,
    mv:r.med_views,ml:(r.med_like*100).toFixed(2)+'%',mc:(r.med_cmt*100).toFixed(2)+'%',shorts:r.shortPct+'%',
    sv:'',sl:'',sc:'',ver:'verified live',src:'new discovery',batch:'',found:'2026-09-15'});
});
const tier=r=>{const v=N(r.mv||r.sv),l=parseFloat(r.ml||r.sl),c=parseFloat(r.mc||r.sc);
  if(v>=20000&&l>=8&&c>=1)return 'A · full spec';
  if(v>=20000&&l>=8)return 'B · 20k+ & 8% like';
  if(v>=20000&&l>=5)return 'C · 20k+ & 5% like';
  if(v>=20000)return 'D · 20k+ views';
  if(v>=10000)return 'E · 10k-20k views';
  return 'F · under 10k';};
rows.forEach(r=>r.tier=tier(r));
const ord=t=>'ABCDEF'.indexOf(t[0]);
rows.sort((a,b)=>ord(a.tier)-ord(b.tier)||(a.fit==='yes'?0:1)-(b.fit==='yes'?0:1)||N(b.mv||b.sv)-N(a.mv||a.sv));

const HD=['Rank','Tier','Email','Verified','Niche Fit','Fit Flag','Name','Handle','Channel URL','Niche / Found Via','Subscribers','Median Views','Median Like %','Median Comment %','% Shorts','Avg Views (bot)','Like % (bot)','Comment % (bot)','Source','Batch','Date Found'];
const data=rows.map((r,i)=>[i+1,r.tier,r.email,r.ver,r.fit,r.flag,r.name,r.handle,r.url,r.niche,r.subs,r.mv,r.ml,r.mc,r.shorts,r.sv,r.sl,r.sc,r.src,r.batch,r.found]);
const esc=v=>'"'+String(v??'').replace(/"/g,'""')+'"';
const OUT='C:/Users/josen/OneDrive/Escritorio/Claude Test/';
fs.writeFileSync(OUT+'bemellou-ALL-emails.csv',[HD.map(esc).join(','),...data.map(r=>r.map(esc).join(','))].join('\n'));
try{
  const X=require('xlsx');
  const ws=X.utils.aoa_to_sheet([HD,...data]);
  ws['!cols']=HD.map((h,i)=>({wch:[6,20,34,17,17,10,18,26,42,26,12,13,14,16,9,15,12,14,14,7,11][i]||12}));
  ws['!autofilter']={ref:X.utils.encode_range({s:{r:0,c:0},e:{r:data.length,c:HD.length-1}})};
  ws['!freeze']={xSplit:0,ySplit:1};
  const wb=X.utils.book_new();X.utils.book_append_sheet(wb,ws,'All Emails');
  X.writeFile(wb,OUT+'bemellou-ALL-emails.xlsx');
  console.log('xlsx written');
}catch(e){console.log('xlsx skipped: '+e.message)}
console.log('\nTOTAL UNIQUE EMAILS: '+rows.length);
const T={};rows.forEach(r=>T[r.tier]=(T[r.tier]||0)+1);
console.log('\ntier'.padEnd(23)+'emails   niche-fit');
Object.keys(T).sort().forEach(k=>console.log(k.padEnd(22)+String(T[k]).padStart(6)+String(rows.filter(r=>r.tier===k&&r.fit==='yes').length).padStart(11)));
console.log('\nverified live in bio: '+rows.filter(r=>r.ver==='verified live').length);
console.log('niche-fit total: '+rows.filter(r=>r.fit==='yes').length);
