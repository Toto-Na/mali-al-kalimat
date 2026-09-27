import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import crypto from 'node:crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const dist = path.join(__dirname, 'dist');
const rooms = new Map();

const DEFAULT_COLUMNS = [
  { id:'name', label:'اسم', icon:'👤' },
  { id:'plant', label:'نبات', icon:'🌿' },
  { id:'animal', label:'حيوان', icon:'🐾' },
  { id:'thing', label:'جماد', icon:'📦' },
  { id:'country', label:'بلاد', icon:'🌍' }
];
const LETTERS = ['ا','ب','ت','ث','ج','ح','خ','د','ذ','ر','ز','س','ش','ص','ض','ط','ظ','ع','غ','ف','ق','ك','ل','م','ن','ه','و','ي'];
const DEFAULT_ROUND_SECONDS = 60, MIN_ROUND_SECONDS = 15, MAX_ROUND_SECONDS = 300;
const roomCode = () => crypto.randomBytes(3).toString('hex').toUpperCase();
const playerId = () => crypto.randomUUID();
const randomLetter = () => LETTERS[Math.floor(Math.random() * LETTERS.length)];
const cleanName = n => String(n || 'لاعب').trim().replace(/[<>]/g,'').slice(0,24) || 'لاعب';
const cleanRoom = r => String(r || '').trim().toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,6);
const cleanColumn = (label, index) => {
  const text = String(label || '').trim().replace(/[<>]/g,'').slice(0,24);
  if (!text) return null;
  const key = text.trim().toLowerCase();
  const icons = {
    'ولد':'👦','بنت':'👧','مدينة':'🏙️','بلد':'🌍','بلاد':'🌍','دولة':'🌍',
    'أكلة':'🍽️','اكلة':'🍽️','اكل':'🍽️','طعام':'🍽️','مهنة':'💼','وظيفة':'💼',
    'سيارة':'🚗','سيارات':'🚗','لون':'🎨','فاكهة':'🍎','فاكه':'🍎','خضار':'🥕',
    'شراب':'🥤','حيوان':'🐾','نبات':'🌿','جماد':'📦','اسم':'👤','رياضة':'⚽',
    'هواية':'🎯','ملابس':'👕','مادة':'📚','دراسة':'📚','شركة':'🏢','أداة':'🛠️',
    'اداة':'🛠️','بحر':'🌊','مكان':'📍','عملة':'💰','موسيقى':'🎵','فيلم':'🎬'
  };
  return { id:`custom_${Date.now()}_${index}_${Math.random().toString(36).slice(2,7)}`, label:text.slice(0,24), icon:icons[key] || '📋' };
};
const getPlayers = room => [...room.players.values()].map(p => ({
  id:p.id,name:p.name,host:p.id===room.hostId,submitted:room.submissions.has(p.id)
}));
const send = (ws,msg) => { if(ws.readyState===1) ws.send(JSON.stringify(msg)); };
const broadcast = (room,msg) => {
  const data=JSON.stringify(msg);
  for(const p of room.players.values()) if(p.ws.readyState===1) p.ws.send(data);
};
const uniqueRoomCode = () => { let c; do c=roomCode(); while(rooms.has(c)); return c; };
const makeRoom = () => {
  const code=uniqueRoomCode();
  const room={code,hostId:null,players:new Map(),round:0,started:false,letter:'',deadline:0,
    roundSeconds:DEFAULT_ROUND_SECONDS,columns:DEFAULT_COLUMNS.map(x=>({...x})),submissions:new Map(),lastResults:[]};
  rooms.set(code,room); return room;
};
const normalizeAnswer = value => String(value||'').trim().toLocaleLowerCase('ar')
  .replace(/[ًٌٍَُِّْـ]/g,'').replace(/[أإآ]/g,'ا').replace(/ة/g,'ه').replace(/ى/g,'ي')
  .replace(/[^\p{L}\p{N} ]/gu,'').replace(/\s+/g,' ');
const scoreAnswer = (value,letter) => {
  const v=String(value||'').trim(), n=normalizeAnswer(v), l=normalizeAnswer(letter);
  if(!v || n.length < 2 || !n.startsWith(l)) return 0;
  return 10;
};
function finishRound(room){
  if(!room.started) return;
  room.started=false;
  const all=[...room.players.values()].map(p=>({p,answers:room.submissions.get(p.id)||{}}));
  const results=all.map(({p,answers})=>{
    const breakdown={}, details={};
    for(const c of room.columns){
      const answer=String(answers[c.id]||'').trim();
      const base=scoreAnswer(answer,room.letter), key=normalizeAnswer(answer);
      const duplicate=Boolean(base && all.some(({p:o,answers:a})=>o.id!==p.id && normalizeAnswer(a[c.id])===key));
      breakdown[c.id]=base ? (duplicate?5:10) : 0;
      details[c.id]={answer,points:breakdown[c.id],duplicate,valid:Boolean(base),empty:!answer,
        reason:!answer?'فارغة':!base?(normalizeAnswer(answer).length<2?'حرف واحد فقط':'لا تبدأ بالحرف المطلوب'):duplicate?'كلمة مكررة':'إجابة صحيحة'};
    }
    const score=Object.values(breakdown).reduce((a,b)=>a+b,0);
    return {id:p.id,name:p.name,score,breakdown,answers,details};
  }).sort((a,b)=>b.score-a.score||a.name.localeCompare(b.name,'ar'));
  room.lastResults=results;
  broadcast(room,{type:'roundResult',results,columns:room.columns,letter:room.letter,round:room.round});
}
function broadcastRoom(room){
  broadcast(room,{type:'players',players:getPlayers(room),hostId:room.hostId});
  broadcast(room,{type:'columns',columns:room.columns});
}
function startRound(room){
  room.round++;
  room.letter=randomLetter();
  room.started=true;
  room.submissions=new Map();
  room.deadline=Date.now()+room.roundSeconds*1000;
  broadcast(room,{type:'game',letter:room.letter,round:room.round,seconds:room.roundSeconds,deadline:room.deadline,columns:room.columns});
  broadcastRoom(room);
}
function transferHost(room){ if(!room.players.has(room.hostId)) room.hostId=room.players.keys().next().value||null; }
function serveStatic(req,res){
  const url=new URL(req.url,`http://${req.headers.host||'localhost'}`);
  if(url.pathname==='/health'){res.writeHead(200,{'content-type':'application/json; charset=utf-8'});return res.end(JSON.stringify({ok:true,rooms:rooms.size,uptime:process.uptime()}));}
  let relative=decodeURIComponent(url.pathname).replace(/^\/+/,'')||'index.html';
  let file=path.resolve(dist,relative);
  if(!file.startsWith(path.resolve(dist))||!fs.existsSync(file)||fs.statSync(file).isDirectory()) file=path.join(dist,'index.html');
  if(!fs.existsSync(file)){res.writeHead(503,{'content-type':'text/plain; charset=utf-8'});return res.end('React build is missing. Run: npm run build');}
  const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml','.json':'application/json'};
  res.writeHead(200,{'content-type':types[path.extname(file)]||'application/octet-stream','cache-control':path.basename(file)==='index.html'?'no-cache':'public, max-age=31536000, immutable'});
  fs.createReadStream(file).pipe(res);
}
const server=http.createServer(serveStatic);
const wss=new WebSocketServer({server});
wss.on('connection',ws=>{
  let player=null,room=null;
  const leave=()=>{
    if(!player||!room)return;
    room.players.delete(player.id); room.submissions.delete(player.id);
    if(room.hostId===player.id)transferHost(room);
    if(room.players.size){broadcastRoom(room);} else rooms.delete(room.code);
    player=null;room=null;
  };
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw.toString())}catch{return send(ws,{type:'error',message:'طلب غير صالح.'})}
    if(m.action==='create'||m.action==='join'){
      if(player)return;
      const name=cleanName(m.name); let target;
      if(m.action==='create')target=makeRoom();
      else{
        const code=cleanRoom(m.room);target=rooms.get(code);
        if(!target)return send(ws,{type:'error',message:'رمز الغرفة غير صحيح أو الغرفة انتهت.'});
        if(target.started)return send(ws,{type:'error',message:'الجولة بدأت. انتظري الجولة التالية.'});
      }
      if(target.players.size>=20)return send(ws,{type:'error',message:'الغرفة ممتلئة (20 لاعبًا كحد أقصى).'});
      const duplicate=[...target.players.values()].some(p=>p.name===name);
      const finalName=duplicate?`${name} ${target.players.size+1}`:name;
      player={id:playerId(),name:finalName,ws};room=target;room.players.set(player.id,player);
      if(!room.hostId)room.hostId=player.id;
      send(ws,{type:'room',room:room.code,playerId:player.id,hostId:room.hostId,players:getPlayers(room),roundSeconds:room.roundSeconds,columns:room.columns});
      broadcastRoom(room);return;
    }
    if(!player||!room)return send(ws,{type:'error',message:'انضمي إلى غرفة أولًا.'});
    if(m.action==='setColumns'&&player.id===room.hostId&&!room.started){
      const incoming=Array.isArray(m.columns)?m.columns:[];
      if(incoming.length<1||incoming.length>12)return send(ws,{type:'error',message:'يجب أن يكون عدد الأعمدة بين 1 و12.'});
      const cleaned=incoming.map((x,i)=>cleanColumn(x.label,i)).filter(Boolean);
      const seen=new Set();
      room.columns=cleaned.filter(c=>{const k=normalizeAnswer(c.label);if(seen.has(k))return false;seen.add(k);return true;}).slice(0,12);
      if(!room.columns.length)return send(ws,{type:'error',message:'أضيفي عمودًا واحدًا على الأقل.'});
      return broadcastRoom(room);
    }
    if(m.action==='start'&&player.id===room.hostId&&!room.started){
      const requested=Number(m.seconds);
      if(Number.isFinite(requested))room.roundSeconds=Math.min(MAX_ROUND_SECONDS,Math.max(MIN_ROUND_SECONDS,Math.round(requested)));
      return startRound(room);
    }
    if(m.action==='submit'&&room.started&&!room.submissions.has(player.id)){
      const answers={};
      for(const c of room.columns)answers[c.id]=String(m.answers?.[c.id]||'').trim().slice(0,40);
      room.submissions.set(player.id,answers);
      broadcast(room,{type:'players',players:getPlayers(room),hostId:room.hostId});
      if(room.submissions.size>=room.players.size)finishRound(room);
      return;
    }
    if(m.action==='next'&&player.id===room.hostId&&!room.started)return startRound(room);
    if(m.action==='leave')return leave();
  });
  ws.on('close',leave);ws.on('error',()=>{});
});
setInterval(()=>{
  for(const room of rooms.values()){
    if(!room.started)continue;
    const remaining=Math.max(0,Math.ceil((room.deadline-Date.now())/1000));
    broadcast(room,{type:'tick',seconds:remaining,deadline:room.deadline});
    if(remaining<=0)finishRound(room);
  }
},1000);
server.listen(PORT,HOST,()=>console.log(`Mali Al Kalimat server listening on ${HOST}:${PORT}`));
