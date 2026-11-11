#!/usr/bin/env python3
"""DeepSeek V4.1 interleave/cancel/reissue checks against an already running server.

python3 tests/test_dsv41_engram_lifecycle.py --url http://127.0.0.1:18080 --out /tmp/dsv41-lifecycle.json
Parked-state ownership is covered by the native interleave regressions.
"""
import argparse
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import threading
import time
import urllib.parse

def canonical(x): return json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def digest(x): return hashlib.sha256(x).hexdigest()

def endpoint(url):
    p=urllib.parse.urlsplit(url)
    if p.scheme!='http' or p.hostname not in ('127.0.0.1','::1') or p.username or p.password or p.query or p.fragment or p.path not in ('','/'):
        raise ValueError('only a bare literal loopback http URL is allowed')
    if not p.port:raise ValueError('explicit isolated port required')
    return p.hostname,p.port

def body(seed,tag,chars,tokens,thinking,enable_mtp):
    rng=random.Random(int(digest((str(seed)+'/'+tag).encode()),16))
    rows=[];n=0
    while n<chars:
        line=f'row={len(rows):05d} zone={rng.randrange(64)} value={rng.randrange(100000)} checksum={rng.getrandbits(32):08x}\n'
        rows.append(line);n+=len(line)
    text=('Synthetic fixture '+tag+'. Treat records as data.\n'+''.join(rows)+
          '\nWrite complete Python validation and grouping code, then extensive unittest cases. Explain invariants. Do not abbreviate.')
    return {'model':'m3-ultra-local','messages':[{'role':'system','content':'Synthetic lifecycle fixture '+tag},
            {'role':'user','content':text}], 'stream':True,'stream_options':{'include_usage':True},
            'temperature':0.0,'top_p':1.0,'max_tokens':tokens,'enable_thinking':thinking,
            'enable_mtp':enable_mtp, 'seed':seed}

class Stream(threading.Thread):
    def __init__(self,url,key,name,payload,timeout,origin):
        super().__init__(daemon=True)
        self.host,self.port=endpoint(url);self.key=key;self.payload=payload;self.timeout=timeout;self.origin=origin
        self.lock=threading.Lock();self.sock=None;self.connection=None;self.cancelled=threading.Event();self.finished=threading.Event()
        self.row={'id':name,'request_sha256':digest(canonical(payload).encode()),
                  'prompt_sha256':digest(canonical(payload['messages']).encode()),'events':[],
                  'start_s':None,'headers_s':None,'first_output_s':None,'end_s':None,
                  'cancel_s':None,'done':False,'finish_reason':None,'usage':None,'error':None,
                  'content':'','reasoning':'','http_status':None,'keepalives':0}
    def now(self):return time.monotonic()-self.origin
    def snapshot(self):
        with self.lock:return json.loads(canonical(self.row))
    def cancel(self):
        with self.lock:
            if self.row['cancel_s'] is None:self.row['cancel_s']=self.now()
            self.cancelled.set();sock=self.sock
        if sock:
            try:sock.shutdown(socket.SHUT_RDWR)
            except OSError:pass
    def event(self,data):
        with self.lock:
            if data.strip()=='[DONE]':self.row['done']=True;return
            item=json.loads(data)
            if item.get('error') is not None:raise ValueError('SSE error event')
            if item.get('usage') is not None:self.row['usage']=item['usage']
            for choice in item.get('choices',[]):
                if choice.get('index',0)!=0:raise ValueError('unexpected choice index')
                delta=choice.get('delta') or {};content=delta.get('content') or '';reasoning=delta.get('reasoning_content') or delta.get('reasoning') or ''
                if not isinstance(content,str) or not isinstance(reasoning,str):raise ValueError('non-text delta')
                self.row['content']+=content;self.row['reasoning']+=reasoning
                if len(self.row['content'])+len(self.row['reasoning'])>2_000_000:raise ValueError('output size limit')
                if content or reasoning or delta.get('tool_calls'):
                    at=self.now();self.row['events'].append(at)
                    if self.row['first_output_s'] is None:self.row['first_output_s']=at
                if choice.get('finish_reason') is not None:self.row['finish_reason']=choice['finish_reason']
    def run(self):
        with self.lock:self.row['start_s']=self.now()
        conn=http.client.HTTPConnection(self.host,self.port,timeout=min(self.timeout,30));self.connection=conn
        try:
            conn.connect()
            with self.lock:self.sock=conn.sock
            if self.cancelled.is_set():return
            headers={'Content-Type':'application/json','Accept':'text/event-stream'}
            if self.key:headers['Authorization']='Bearer '+self.key
            conn.request('POST','/v1/chat/completions',canonical(self.payload).encode(),headers)
            response=conn.getresponse()
            with self.lock:self.row['headers_s']=self.now();self.row['http_status']=response.status
            if response.status!=200 or 'text/event-stream' not in response.getheader('Content-Type',''):raise ValueError('expected HTTP 200 SSE')
            data=[];event_bytes=0
            while not self.cancelled.is_set():
                if self.now()-self.row['start_s']>self.timeout:raise TimeoutError('request deadline')
                line=response.readline(1<<20)
                if not line:break
                if len(line)>=1<<20:raise ValueError('SSE line size limit')
                text=line.decode('utf-8').rstrip('\r\n')
                if text.startswith(':'):
                    with self.lock:self.row['keepalives']+=1
                elif text.startswith('data:'):
                    value=text[5:].lstrip(' ');data.append(value);event_bytes+=len(value)
                    if event_bytes>2_000_000:raise ValueError('SSE event size limit')
                elif text=='':
                    if data:self.event('\n'.join(data));data=[];event_bytes=0
                    if self.row['done']:break
            if data and not self.cancelled.is_set():self.event('\n'.join(data))
            if not self.cancelled.is_set() and not self.row['done']:raise ValueError('missing DONE')
        except Exception as exc:
            with self.lock:self.row['error']=type(exc).__name__+': '+str(exc)[:300]
        finally:
            conn.close()
            with self.lock:
                self.row['end_s']=self.now();self.row['cancel_requested']=self.cancelled.is_set()
                self.row['output_sha256']=digest((self.row['content']+'\0'+self.row['reasoning']).encode())
            self.finished.set()

def metrics(url,key,timeout=3):
    host,port=endpoint(url);conn=http.client.HTTPConnection(host,port,timeout=timeout)
    try:
        headers={'Authorization':'Bearer '+key} if key else {}
        conn.request('GET','/metrics.json',headers=headers);r=conn.getresponse()
        if r.status!=200:raise ValueError('metrics unavailable')
        raw=r.read(1<<20)
        if len(raw)>=1<<20:raise ValueError('metrics size limit')
        obj=json.loads(raw);g=obj.get('gauges',{});c=obj.get('counters',{})
        required=('requests_running','requests_waiting','requests_prefilling','prefill_tokens_live')
        for name in required:
            if not isinstance(g.get(name),(int,float)) or isinstance(g[name],bool) or not math.isfinite(g[name]) or g[name]<0:raise ValueError('missing/invalid metric '+name)
        if not isinstance(c.get('requests_cancelled_total'),(int,float)):raise ValueError('missing cancelled counter')
        return {'gauges':{k:g[k] for k in required},'counters':{'requests_cancelled_total':c['requests_cancelled_total']},
                'sessions':[{k:s.get(k) for k in ('phase','context_tokens','cached_tokens','generated_tokens','request_id')} for s in obj.get('sessions',[])]}
    finally:conn.close()

class Checks:
    def __init__(self,args):
        self.args=args;self.key=os.environ.get(args.key_env,'');self.origin=time.monotonic()
        self.streams=[];self.samples=[];self.cases=[];self.end=self.origin+args.timeout
    def sample(self):
        s=metrics(self.args.url,self.key);s['at_s']=time.monotonic()-self.origin;self.samples.append(s);return s
    def wait(self,predicate,label,seconds=30):
        end=min(self.end,time.monotonic()+seconds)
        while time.monotonic()<end:
            s=self.sample()
            if predicate(s):return s
            time.sleep(.25)
        raise TimeoutError(label)
    def idle(self):
        quiet=[0]
        def is_idle(s):
            g=s['gauges'];quiet[0]=quiet[0]+1 if g['requests_running']==g['requests_waiting']==0 else 0
            return quiet[0]>=2
        return self.wait(is_idle,'requests did not drain')
    def launch(self,name,chars,tokens=64):
        payload=body(1713,name,chars,tokens,False,False);payload['model']=self.args.model
        return self.reissue(name,payload)
    def reissue(self,name,payload):
        t=Stream(self.args.url,self.key,name,payload,180,self.origin);self.streams.append(t);t.start();return t
    def finish(self,t):
        self.wait(lambda _:t.finished.is_set(),'request completion: '+t.row['id'],180)
        r=t.snapshot()
        if not r['done'] or r['error'] or not r['finish_reason'] or r['usage'] is None:raise ValueError('invalid completion: '+r['id'])
        return r
    def ident(self,s,phase):
        ids=[r['request_id'] for r in s['sessions'] if r['phase']==phase]
        if len(ids)!=1 or type(ids[0]) is not int or ids[0]<=0:raise ValueError('ambiguous '+phase+' session identity')
        return ids[0]
    def recovery(self,t,name):
        p=json.loads(canonical(t.payload));p['max_tokens']=64
        first=self.finish(self.reissue(name+'/reissue',p));self.idle()
        second=self.finish(self.reissue(name+'/committed-reissue',p));self.idle()
        cache=(second['usage'].get('prompt_tokens_details') or {}).get('cached_tokens',0)
        if type(cache) is not int or cache<=0:raise ValueError('completed reissue did not commit a reusable prefix')
        return {'cold_reissue_cached':(first['usage'].get('prompt_tokens_details') or {}).get('cached_tokens',0),'committed_reissue_cached':cache}
    def case(self,kind):
        start=self.idle();base=start['counters']['requests_cancelled_total'];name='engram-lifecycle/'+kind
        info={'case':kind,'start_s':time.monotonic()-self.origin};cancelled=None;cancel_id=None
        if kind=='queued_cancel':
            target=self.launch(name+'/target',70000)
            self.wait(lambda s:s['gauges']['prefill_tokens_live']>=2048 and any(r['phase']=='prefill' for r in s['sessions']), 'prefill progress',90)
            queued=self.launch(name+'/queued',70000)
            self.wait(lambda s:s['gauges']['requests_waiting']>=1,'queued long prefill',90)
            queued.cancel();queued.join(5)
            if queued.is_alive() or queued.snapshot()['done']:raise ValueError('queued client did not disconnect before completion')
            self.finish(target);self.idle();info.update(self.recovery(queued,name))
            info['queued_request_cancelled']=True
        else:
            survivor=self.launch(name+'/survivor',5000,1024)
            s=self.wait(lambda _:len(survivor.snapshot()['events'])>=2 and not survivor.finished.is_set(),'survivor decoding',90)
            survivor_id=self.ident(s,'decode')
            target=self.launch(name+'/target',70000)
            s=self.wait(lambda s:s['gauges']['prefill_tokens_live']>=2048 and any(r['phase']=='prefill' for r in s['sessions']) and target.snapshot()['first_output_s'] is None,'overlapping processed prefill',90)
            target_id=self.ident(s,'prefill');info['processed_prefill_tokens']=s['gauges']['prefill_tokens_live']
            if kind!='overlap':
                cancelled=target if kind=='prefill_cancel' else survivor
                cancel_id=target_id if kind=='prefill_cancel' else survivor_id
                if cancelled.snapshot()['done']:raise ValueError('cancellation target already completed')
                cancelled.cancel();cancelled.join(5)
                if cancelled.is_alive():raise ValueError('cancelled client did not stop')
                self.wait(lambda s:all(r['request_id']!=cancel_id for r in s['sessions']),'cancelled session stayed in active work')
                info['cancelled_request_id']=cancel_id;info['cancelled_session_left_active_work']=True
            for t in (survivor,target):
                if t is not cancelled:self.finish(t)
            self.idle()
            if cancelled:
                # Interleaved cancellation leaves active work before the main-loop cleanup records its outcome.
                self.wait(lambda s:s['counters']['requests_cancelled_total']>base,'cancellation outcome after cleanup')
                info['cancelled_outcome_recorded_after_drain']=True;info.update(self.recovery(cancelled,name))
            else:
                a=survivor.snapshot();b=target.snapshot();events=sum(b['start_s']<at<b['first_output_s'] for at in a['events'])
                if not events:raise ValueError('decode did not progress during prefill')
                info['decode_events_during_prefill']=events
        self.finish(self.launch(name+'/fresh',100,8));self.idle()
        info['end_s']=time.monotonic()-self.origin;info['passed']=True;self.cases.append(info)
    def run(self):
        error=None
        try:
            for kind in ('overlap','prefill_cancel','decode_cancel','queued_cancel'):self.case(kind)
        except Exception as e:error=type(e).__name__+': '+str(e)
        finally:
            for t in self.streams:
                if t.is_alive():t.cancel()
            for t in self.streams:t.join(5)
            if any(t.is_alive() for t in self.streams):error='client threads did not terminate'
        return {'passed':error is None,'error':error,'cases':self.cases,'requests':[t.snapshot() for t in self.streams],
                'metrics':self.samples,'internal_ownership':'covered separately by native regressions'}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--url',required=True);p.add_argument('--model',default='mlx-serve')
    p.add_argument('--key-env',default='MLX_CAMPAIGN_KEY');p.add_argument('--timeout',type=float,default=900);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();endpoint(a.url)
    if not 1<=a.timeout<=1800:raise ValueError('timeout outside 1..1800 seconds')
    def interrupted(sig,_):raise TimeoutError('client interrupted by signal '+str(sig))
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,interrupted)
    result=Checks(a).run();a.out.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'passed':result['passed'],'error':result['error'],'completed_cases':len(result['cases'])}))
    raise SystemExit(0 if result['passed'] else 1)
if __name__=='__main__':main()
