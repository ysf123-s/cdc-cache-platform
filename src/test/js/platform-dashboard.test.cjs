const fs = require('fs');
const vm = require('vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync('src/main/resources/static/platform-dashboard.html', 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map();
const requests = [];
const document = {
  querySelectorAll() { return []; },
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, {value:'', innerHTML:'', textContent:'', disabled:true, checked:false, style:{},
      listeners:{}, addEventListener(name, fn) { this.listeners[name] = fn; }});
    return elements.get(id);
  }
};
const event = (n) => ({sequence:String(n),eventId:n.toString(16).padStart(64,'0'),table:'inventory',operation:'INSERT',
  primaryKeyValue:n === 21 ? '<img src=x onerror=alert(1)>' : String(n),eventTimeMillis:1700000000000});
const sub = {subscriptionId:'a'.repeat(64),table:'inventory',targetName:'hook',operations:'UPDATE',changedFields:'name,available',
  filterField:'name',filterValue:'<img src=x onerror=alert(1)>',ruleVersion:2,enabled:true};
let pageCalls = 0;
let stockoutHandled = false;
let enabledExamples = [];
const context = {document, window:{confirm:()=>true,location:{search:''}}, URLSearchParams, Date, JSON, console, fetch:async(url,opts={}) => {
  requests.push({url,...opts});
  let data;
  if (url === '/platform/examples') data=enabledExamples;
  else if (url === '/platform/tables') data=[];
  else if (url === '/platform/targets') data=['hook'];
  else if (url.startsWith('/platform/events?')) {
    pageCalls++;
    data=url.includes('cursor=2') ? {items:[event(1)],nextCursor:null}
      : {items:Array.from({length:20},(_,i)=>event(21-i)),nextCursor:'2'};
  } else if (url === '/platform/subscriptions') data = opts.method ? sub : [sub];
  else if (url.startsWith('/platform/subscriptions/')) data=sub;
  else if (url.startsWith('/platform/deliveries?')) {
    const query = new URL(url, 'http://localhost').searchParams;
    const before = Number(query.get('cursor') || 126);
    const tasks = Array.from({length:125}, (_,i) => ({taskId:String(125-i), deliveryId:'d'.repeat(64),
      eventId:event(125-i).eventId, targetName:'<img src=x onerror=alert(1)>', roundType:'INITIAL',
      status:125-i > 75 ? 'SUCCEEDED' : 'RETRY_WAIT', attemptCount:1,maxAttempts:3}))
      .filter(t => Number(t.taskId) < before && (!query.get('status') || query.get('status') === t.status)
        && (!query.get('eventId') || query.get('eventId') === t.eventId));
    data={items:tasks.slice(0,50), nextCursor:tasks.length > 50 ? tasks[49].taskId : null};
  }
  else if (url.startsWith('/platform/deliveries/') && url.endsWith('/attempts')) data=[{attemptNumber:1,httpStatus:503,errorMessage:'unavailable'}];
  else if (url.startsWith('/warehouse/stockout-tasks?')) {
    const q=new URL(url,'http://localhost').searchParams;
    let rows=Array.from({length:21},(_,i)=>({id:String(21-i),inventoryId:'3',sku:'<img src=x onerror=alert(1)>',
      eventId:event(21-i).eventId,eventTimeMillis:1700000000000,status:stockoutHandled&&i===0?'HANDLED':'PENDING'}));
    rows=rows.filter(t=>(!q.get('cursor')||Number(t.id)<Number(q.get('cursor')))
      &&(!q.get('eventId')||t.eventId===q.get('eventId'))&&(!q.get('status')||t.status===q.get('status')));
    data={items:rows.slice(0,20),nextCursor:rows.length>20?rows[19].id:null};
  }
  else if (url === '/warehouse/stockout-tasks/21/complete') { stockoutHandled=true;data={status:'HANDLED'}; }
  else if (url.startsWith('/platform/events/')) data={eventId:url.split('/').at(-1)};
  else if (url === '/platform/recovery-plans') data={planId:'b'.repeat(64),executable:true,reason:'Pre-check passed',generatedSql:'-- preview',checkSummary:'{}'};
  else throw Error('Unexpected request: '+url);
  return {ok:true,headers:{get:()=> 'application/json'},json:async()=>data};
}};
vm.createContext(context);vm.runInContext(source,context);
(async () => {
  await context.init();
  assert.equal(document.getElementById('stockoutDemoTab').hidden,true);
  assert.ok(!document.getElementById('deliveryList').innerHTML.includes('viewEventStockouts'));
  await context.resetSearch();
  assert.ok(!document.getElementById('eventList').innerHTML.includes('viewEventStockouts'));
  assert.ok(!requests.some(r=>r.url.startsWith('/warehouse/')));
  assert.equal(document.getElementById('btnNextCursor').disabled,false);
  assert.equal(document.getElementById('btnPrevCursor').disabled,true);
  assert.ok(!document.getElementById('eventList').innerHTML.includes('<img'));
  await context.nextCursor();
  assert.ok(requests.at(-1).url.includes('cursor=2'));
  assert.equal(document.getElementById('btnPrevCursor').disabled,false);
  assert.equal(document.getElementById('btnNextCursor').disabled,true);
  await context.prevCursor();
  assert.ok(!requests.at(-1).url.includes('cursor='));
  document.getElementById('filterKey').listeners.change();
  assert.equal(document.getElementById('btnNextCursor').disabled,true);
  await context.loadSubscriptions();
  assert.ok(!document.getElementById('subList').innerHTML.includes('<img'));
  await context.toggleSub(sub.subscriptionId,false,2);
  const toggle=requests.find(r=>r.url.includes('/enabled?'));
  assert.equal(toggle.method,'PUT');assert.deepEqual(JSON.parse(toggle.body),{enabled:false});
  context.editSub(sub.subscriptionId);
  document.getElementById('newSubChanged').value='';
  document.getElementById('newSubFilterField').value='';
  await context.createSubscription();
  const edit=requests.find(r=>r.method === 'PUT' && !r.url.includes('/enabled?'));
  assert.equal(JSON.parse(edit.body).filterField,null);
  assert.equal(JSON.parse(edit.body).changedFields,null);
  document.getElementById('recoveryEventId').value='a'.repeat(64);
  await context.generatePlan();
  assert.ok(document.getElementById('planExecutable').textContent.includes('仍需复核'));
  assert.equal(pageCalls,3);
  await context.resetDeliveries();
  assert.equal(document.getElementById('cardTasks').textContent,0);
  assert.equal(document.getElementById('btnNextDelivery').disabled,false);
  assert.equal(document.getElementById('btnPrevDelivery').disabled,true);
  assert.ok(!document.getElementById('deliveryList').innerHTML.includes('<img'));
  await context.nextDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=76'));
  assert.equal(document.getElementById('cardTasks').textContent,50);
  await context.nextDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=26'));
  assert.equal(document.getElementById('cardTasks').textContent,25);
  assert.equal(document.getElementById('btnNextDelivery').disabled,true);
  await context.prevDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=76'));
  document.getElementById('delivStatus').value='RETRY_WAIT';
  await document.getElementById('delivStatus').listeners.change();
  assert.ok(requests.at(-1).url.includes('status=RETRY_WAIT'));
  assert.ok(!requests.at(-1).url.includes('cursor='));
  assert.equal(document.getElementById('btnPrevDelivery').disabled,true);
  context.showTab = () => {};
  await context.viewEventDeliveries(event(1).eventId);
  assert.ok(requests.at(-1).url.includes('eventId='+event(1).eventId));
  assert.equal(document.getElementById('cardTasks').textContent,1);
  const html = fs.readFileSync('src/main/resources/static/platform-dashboard.html','utf8');
  assert.ok(html.includes('当前页待处理/重试'));
  assert.ok(html.includes('追加一次投递'));
  await context.loadAttempts('1');
  assert.ok(document.getElementById('attemptDetail').textContent.includes('503'));
  document.getElementById('replayJobId').value='b'.repeat(64);
  await context.viewReplayDeliveries();
  assert.ok(requests.at(-1).url.includes('roundId='+'b'.repeat(64)));
  assert.ok(!requests.at(-1).url.includes('eventId='));
  enabledExamples=['stockout'];
  await context.init();
  assert.equal(document.getElementById('stockoutDemoTab').hidden,false);
  assert.ok(document.getElementById('deliveryList').innerHTML.includes('viewEventStockouts'));
  await context.resetSearch();
  assert.ok(document.getElementById('eventList').innerHTML.includes('viewEventStockouts'));
  document.getElementById('stockoutStatus').value='PENDING';
  await context.resetStockouts();
  assert.equal(document.getElementById('stockoutNext').disabled,false);
  assert.ok(!document.getElementById('stockoutList').innerHTML.includes('<img'));
  await context.nextStockout();
  assert.ok(requests.at(-1).url.includes('cursor=2'));
  assert.equal(document.getElementById('stockoutNext').disabled,true);
  await context.prevStockout();
  await context.completeStockout('21');
  assert.equal(stockoutHandled,true);
  assert.ok(!document.getElementById('stockoutList').innerHTML.includes("completeStockout('21')"));
  await context.viewEventStockouts(event(21).eventId);
  assert.ok(document.getElementById('stockoutList').innerHTML.includes('已处理'));
  await context.openEvent(event(21).eventId);
  assert.ok(document.getElementById('eventDetail').textContent.includes(event(21).eventId));
  console.log('PASS: 21-row pagination, filter reset, text escaping, enabled request, rule editing, recovery wording');
  console.log('PASS: 125-task pagination, pending count scope, delivery filters and event linkage');
  console.log('PASS: replay-round linkage, 21-stockout pagination, completion, source event and escaped SKU');
  console.log('PASS: stockout example disabled by default, no warehouse requests until enabled, conditional entry links');
})().catch(error=>{console.error(error);process.exitCode=1;});
