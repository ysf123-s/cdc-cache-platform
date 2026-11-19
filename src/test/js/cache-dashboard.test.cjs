const fs=require('fs');
const vm=require('vm');
const assert=require('node:assert/strict');
const elements=new Map();
class Element {
  constructor(){this.value='';this.textContent='';this.innerHTML='';this.disabled=false;this.hidden=false;this.children=[];this.listeners={};this.attributes={};this.classList={toggle(){}};}
  replaceChildren(){this.children=[];}
  append(child){this.children.push(child);}
  addEventListener(name,fn){this.listeners[name]=fn;}
  setAttribute(name,value){this.attributes[name]=value;}
  removeAttribute(name){delete this.attributes[name];}
  scrollIntoView(){this.scrolled=true;}
}
const document={getElementById(id){if(!elements.has(id))elements.set(id,new Element());return elements.get(id);},createElement(){return new Element();}};
document.getElementById('table').value='departments';
document.getElementById('scanCount').value='20';
const requests=[];
let forcedStatus=0;
const overview={groups:[{table:'departments',keyTemplate:'dept:#id',expireSeconds:300,nullExpireSeconds:30,hits:1,misses:2,hitRate:1/3,databaseCalls:2,databaseIds:2,coalescedLoads:0}],
  processedEvents:3,failedBatches:0,deletedKeys:2,deadLetters:4,capturedAt:'2026-11-16T09:04:42Z',sync:{serviceStarted:true,activeConsumers:6,configuredConsumers:6,lastSuccessAt:null,lastError:null}};
const entry={table:'departments',id:'1',key:'dept:1',ttlSeconds:100,hash:{data:JSON.stringify({id:'1',name:'<img src=x onerror=alert(1)>',code:'HR'})}};
const deadLetters=[{id:'1-0',fields:{recordedAt:'2026-11-19T07:00:00Z',reason:'MISSING_PRIMARY_KEY',table:'departments',kafkaPartition:'2',kafkaOffset:'9',binlogFile:'mysql-bin.000003',binlogPosition:'120',message:'<b>Missing primary key id</b>'}}];
let confirmed=true;
const context={document,window:{},Date,JSON,console,setInterval(){},confirm:()=>confirmed,fetch:async(url,options={})=>{
  requests.push({url,...options});
  if(forcedStatus)return {ok:false,status:forcedStatus,statusText:'Not Found',text:async()=>'{"error":"Not Found"}'};
  let result;
  if(url==='/cache/overview')result=overview;
  else if(url.includes('/invalidate'))result={requestedKeys:2,deletedKeys:1};
  else if(url.startsWith('/cache/entries/'))result=entry;
  else if(url.endsWith('/purge'))result={table:'departments',deletedKeys:5};
  else if(url==='/cache/dead-letters?count=20')result=deadLetters;
  else if(url.startsWith('/cache/groups/'))result={entries:[{id:'1',key:'dept:1'}],nextCursor:'0'};
  else if(url==='/departments/batch')result={items:[{id:'1',name:'Human Resources',code:'HR'}],missingIds:[2]};
  else if(url==='/departments/1')result={id:'1',name:'Human Resources',code:'HR'};
  else throw Error('Unexpected request '+url);
  return {ok:true,text:async()=>JSON.stringify(result)};
}};
vm.createContext(context);
vm.runInContext(fs.readFileSync('src/main/resources/static/dashboard-ui.js','utf8'),context);
const html=fs.readFileSync('src/main/resources/static/cache-dashboard.html','utf8');
vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
(async()=>{
  await context.overview();
  assert.equal(document.getElementById('deadLetters').textContent,'4');
  confirmed=false;
  await context.purge();
  assert.ok(!requests.some(request=>request.url.endsWith('/purge')));
  confirmed=true;
  await context.purge();
  assert.equal(requests.find(request=>request.url.endsWith('/purge')).method,'POST');
  assert.ok(document.getElementById('notice').textContent.includes('实际删除 5 项'));
  await context.loadDeadLetters();
  assert.ok(document.getElementById('deadLetterList').innerHTML.includes('缺少主键'));
  assert.ok(document.getElementById('deadLetterList').innerHTML.includes('Kafka 2:9'));
  assert.ok(document.getElementById('deadLetterList').innerHTML.includes('mysql-bin.000003:120'));
  assert.ok(!document.getElementById('deadLetterList').innerHTML.includes('<b>'));
  document.getElementById('ids').value='1,2';
  await context.action('query');
  assert.ok(document.getElementById('result').innerHTML.includes('业务查询结果'));
  assert.ok(document.getElementById('result').innerHTML.includes('Human Resources'));
  assert.ok(document.getElementById('result').innerHTML.includes('未找到的 ID：2'));
  assert.ok(document.getElementById('result').innerHTML.includes('<details class="technical">'));
  document.getElementById('ids').value='1';
  await context.action('inspect');
  assert.ok(document.getElementById('result').innerHTML.includes('已缓存业务数据'));
  assert.ok(!document.getElementById('result').innerHTML.includes('<img'));
  context.renderResult('inspect',{...entry,hash:{data:'null'}});
  assert.ok(document.getElementById('result').innerHTML.includes('空值缓存'));
  context.renderResult('inspect',{...entry,ttlSeconds:-2,hash:{}});
  assert.ok(document.getElementById('result').innerHTML.includes('缓存不存在'));
  context.renderResult('inspect',{...entry,ttlSeconds:30,hash:{load_token:'token'}});
  assert.ok(document.getElementById('result').innerHTML.includes('正在加载'));
  await context.action('invalidate');
  assert.ok(document.getElementById('result').innerHTML.includes('实际删除的键'));
  await context.entryAction('departments','9223372036854775800',true,new Element());
  assert.ok(requests.at(-2).body.includes('9223372036854775800'));
  await context.browse('0');
  assert.equal(document.getElementById('entries').children.length,1);
  assert.equal(document.getElementById('nextPage').disabled,true);
  forcedStatus=404;
  await context.action('query');
  assert.ok(document.getElementById('notice').textContent.includes('找不到对应记录'));
  assert.ok(!document.getElementById('notice').textContent.includes('{'));
  assert.equal(document.getElementById('errorTechnical').hidden,false);
  assert.equal(document.getElementById('query').disabled,false);
  assert.ok(html.includes('dashboard-ui.css'));
  console.log('PASS: business result table, missing IDs, cache states, escaped values, invalidation counts, long IDs, readable errors, group purge and dead letters');
})().catch(error=>{console.error(error);process.exitCode=1;});
