import json, os, subprocess, sys, time
from pathlib import Path
from datetime import datetime, timezone
import pymysql
job=Path('/data/wuhao.xwh/seekdb-sirius-compare-20261026')
pipeline=Path('/data/wuhao.xwh/seekdb-sirius-tpch500-20261023')
program='/data/wuhao.xwh/seekdb-sirius-20261022/seekdb/unittest/sql/sirius/path_comparison.py'
projection=job/'customer-projection.duckdb'
oracle=job/'oracle.json'
state={'phases':[], 'status':'waiting_for_statistics'}
def save():
 state['updated_utc']=datetime.now(timezone.utc).isoformat()
 (job/'comparison-state.json').write_text(json.dumps(state,indent=2)+'\n')
 print(json.dumps(state),flush=True)
def run(phase, args):
 state['status']='running';state['phase']=phase;save()
 begin=time.monotonic()
 with (job/(phase+'.log')).open('w') as log:
  subprocess.run([sys.executable,program]+args,stdout=log,stderr=subprocess.STDOUT,check=True)
 state['phases'].append({'name':phase,'wall_seconds':time.monotonic()-begin});save()
save()
try:
 deadline=time.monotonic()+2400
 while '--ready' not in sys.argv:
  status=json.loads((pipeline/'pipeline-state.json').read_text())
  if status['status']=='failed':raise RuntimeError('Full TPC-H preparation failed: '+str(status))
  if status['stage']=='tpch22' and (pipeline/'benchmark/PAUSE').exists():
   with pymysql.connect(host='127.0.0.1',port=2886,user='root',database='test') as conn,conn.cursor() as c:
    c.execute('SHOW FULL PROCESSLIST')
    active=[r for r in c.fetchall() if r[4]=='Query' and 'SHOW FULL PROCESSLIST' not in str(r[7])]
   if not active:break
  if time.monotonic()>deadline:raise TimeoutError('Statistics did not finish before comparison deadline')
  time.sleep(5)
 run('prepare',['prepare','--source','/data/wuhao.xwh/duckdb-sirius-data/tpch_sf500.duckdb','--projection',str(projection),'--output',str(oracle)])
 native=['native','--projection',str(projection),'--oracle',str(oracle),'--extension','/data/wuhao.xwh/duckdb-sirius-upstream/sirius/build/release/extension/sirius/sirius.duckdb_extension']
 run('native-before',native+['--output',str(job/'native-before.json')])
 run('seekdb',['seekdb','--oracle',str(oracle),'--library','/data/wuhao.xwh/seekdb-sirius-20261022/seekdb/build-sirius-arrow/libseekdb_sirius_bridge.so','--socket','/data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock','--tpch-pause',str(pipeline/'benchmark/PAUSE'),'--output',str(job/'seekdb.json')])
 run('native-after',native+['--output',str(job/'native-after.json')])
 state['status']='passed';save()
except BaseException as error:
 state['status']='failed';state['error']=repr(error);save()
 raise
finally:
 backup=Path('/data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock.comparison-backup')
 if backup.exists():
  state['cleanup']='Socket backup requires recovery; keep full TPC-H paused';save()
 else:
  (pipeline/'benchmark/PAUSE').unlink(missing_ok=True)
  state['tpch_pause_removed']=True;save()
