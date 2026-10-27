from pathlib import Path
import json, os, subprocess, time
base=Path('/data/wuhao.xwh/seekdb-sirius-20261022')
job=Path('/data/wuhao.xwh/seekdb-sirius-multigpu-20261027')
pixi=Path('/data/wuhao.xwh/duckdb-sirius-upstream/sirius/.pixi/envs/default')
configs=[(1,'0.15',128),(1,'0.30',128),(2,'0.15',128),(2,'0.30',128),(1,'0.15',384),(2,'0.15',384)]
results=[]
for sweep, sequence in ((1,configs),(2,list(reversed(configs)))):
 for gpus,fraction,chunk in sequence:
  name=f's{sweep}-g{gpus}-f{fraction}-c{chunk}'
  env=dict(os.environ,CUDA_VISIBLE_DEVICES='0' if gpus==1 else '0,1',SEEKDB_PORT='2886',SEEKDB_DATABASE='test',SEEKDB_SIRIUS_CONFIG=str(job/f'g{gpus}-{fraction}.yaml'),LD_LIBRARY_PATH=f'{job}/overlay:{pixi}/lib',PYTHONPATH=str(pixi/'lib/python3.14/site-packages'))
  cmd=[str(base/'venv/bin/python'),str(base/'seekdb/unittest/sql/sirius/multigpu_benchmark.py'),'--library',str(base/'seekdb/build-sirius-arrow/libseekdb_sirius_bridge.so'),'--socket','/data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock','--oracle','/data/wuhao.xwh/seekdb-sirius-compare-20261026/oracle.json','--output',str(job/(name+'.json')),'--chunk-mib',str(chunk),'--repeats','10','--seed',str(20261027+sweep)]
  print(json.dumps({'starting':name,'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}),flush=True)
  with (job/(name+'.log')).open('w') as log:
   subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  report=json.loads((job/(name+'.json')).read_text())
  result={'name':name,'gpus':gpus,'fraction':fraction,'chunk':chunk,'status':report['status'],'cases':{k:v['medians'] for k,v in report['cases'].items()}}
  results.append(result)
  (job/'matrix-state.json').write_text(json.dumps(results,indent=2))
  print(json.dumps(result),flush=True)
