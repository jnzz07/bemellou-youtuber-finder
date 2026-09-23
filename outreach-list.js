const fs=require('fs');const N=x=>Number(x)||0;
const SP=require('path').join(__dirname,'data','legacy')+'/'; // snapshot rescued from a dead session Temp folder (gitignored, holds emails)
const db=require(SP+'all.json');
const emap=require('./emails-map.json');
const med={};[...require('./pool-out.json'),...require('./discovered-out.json')].forEach(r=>{
  if(r.handle)med[r.handle.toLowerCase()]=r;});
const liveE={};[...require(SP+'recovered.json'),...require(SP+'recovered2.json')].forEach(r=>{
  if(r.handle)liveE[r.handle.toLowerCase()]=r;});
const hasE=r=>r.email&&r.email!=='Not listed';
const rows=[];const seen=new Set();
db.forEach(r=>{
  if(!r.vibe||!r.praise||!r.looking_forward)return;
  const h=(r.handle||'').toLowerCase(); const m=med[h]||{}; const lv=liveE[h]||{};
  // best email: fresh bio > live lookup > stored
  const email=(m.id&&emap[m.id]?.email)||lv.newEmail||(hasE(r)?r.email:'');
  if(!email)return;
  // 20k+ gate: verified median if we have it, else the bot's stored average
  const views=m.med_views!=null?m.med_views:N(r.avg_views);
  if(views<20000)return;
  const k=email.toLowerCase(); if(seen.has(k))return; seen.add(k);
  rows.push({email,handle:r.handle,vibe:r.vibe,praise:r.praise,lf:r.looking_forward,views});
});
rows.sort((a,b)=>b.views-a.views);
const esc=v=>'"'+String(v??'').replace(/"/g,'""')+'"';
const HD=['Email','Handle','Vibe','Praise','Looking Forward'];
const data=rows.map(r=>[r.email,r.handle,r.vibe,r.praise,r.lf]);
const OUT='C:/Users/josen/OneDrive/Escritorio/Claude Test/';
fs.writeFileSync(OUT+'bemellou-outreach-20k.csv',[HD.map(esc).join(','),...data.map(r=>r.map(esc).join(','))].join('\n'));
const X=require('xlsx');
const ws=X.utils.aoa_to_sheet([HD,...data]);
ws['!cols']=[{wch:34},{wch:26},{wch:16},{wch:78},{wch:56}];
ws['!autofilter']={ref:X.utils.encode_range({s:{r:0,c:0},e:{r:data.length,c:4}})};
ws['!freeze']={xSplit:0,ySplit:1};
const wb=X.utils.book_new();X.utils.book_append_sheet(wb,ws,'Outreach 20k+');
X.writeFile(wb,OUT+'bemellou-outreach-20k.xlsx');
console.log('TOTAL: '+rows.length+' creators (email + all 3 personalization fields + 20k+ views)');
console.log('  verified median views used: '+rows.filter(r=>med[r.handle.toLowerCase()]?.med_views!=null).length);
console.log('  stored avg views used:      '+rows.filter(r=>med[r.handle.toLowerCase()]?.med_views==null).length);
console.log('\nsample:');
rows.slice(0,3).forEach(r=>console.log('  '+r.handle+' | '+r.email+' | '+r.vibe+' | '+r.praise.slice(0,60)+'...'));
