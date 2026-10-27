from pathlib import Path
import json, os, subprocess, time
base=Path('/data/wuhao.xwh/seekdb-sirius-20261022')
job=Path('/data/wuhao.xwh/seekdb-sirius-multigpu-20261027')
pixi=Path('/data/wuhao.xwh/duckdb-sirius-upstream/sirius/.pixi/envs/default')
previous=int((job/'matrix.pid').read_text())
while Path(f'/proc/{previous}/cmdline').exists() and b'run-matrix.py' in Path(f'/proc/{previous}/cmdline').read_bytes():
 time.sleep(5)
state=json.loads((job/'matrix-state.json').read_text())
assert len(state)==12 and all(r['status']=='passed' for r in state), 'Earlier matrix incomplete'
for gpus in (1,2):
 (job/f'g{gpus}-0.075.yaml').write_text((job/f'g{gpus}-0.15.yaml').read_text().replace('0.15','0.075'))
env=dict(os.environ,CUDA_VISIBLE_DEVICES='0,1',SEEKDB_PORT='2886',SEEKDB_DATABASE='test',SEEKDB_SIRIUS_CONFIG=str(job/'g2-0.075.yaml'),LD_LIBRARY_PATH=f'{job}/overlay:{pixi}/lib',PYTHONPATH=str(pixi/'lib/python3.14/site-packages'),SIRIUS_TEST_LIBRARY=str(job/'bridge/libseekdb_sirius_bridge.so'))
with (job/'unit-tests.log').open('w') as log:
 subprocess.run([str(base/'venv/bin/python'),'-m','unittest','-v','test_native_chunks','test_native_input','test_fragment_budget','test_resident_input','test_multigpu'],cwd=base/'seekdb/unittest/sql/sirius',env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
print('GPU and budget unit tests passed',flush=True)
configs=[(1,'0.15',1536),(2,'0.15',1536),(1,'0.075',1536),(2,'0.075',1536)]
results=[]
for sweep, sequence in ((1,configs),(2,list(reversed(configs)))):
 for gpus,fraction,chunk in sequence:
  name=f'large-s{sweep}-g{gpus}-f{fraction}-c{chunk}'
  env.update(CUDA_VISIBLE_DEVICES='0' if gpus==1 else '0,1',SEEKDB_SIRIUS_CONFIG=str(job/f'g{gpus}-{fraction}.yaml'),SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB='2048')
  cmd=[str(base/'venv/bin/python'),str(base/'seekdb/unittest/sql/sirius/multigpu_benchmark.py'),'--library',str(job/'bridge/libseekdb_sirius_bridge.so'),'--socket','/data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock','--oracle','/data/wuhao.xwh/seekdb-sirius-compare-20261026/oracle.json','--output',str(job/(name+'.json')),'--chunk-mib',str(chunk),'--repeats','10','--seed',str(20261029+sweep)]
  if sweep==1 and gpus==2 and fraction=='0.075': cmd.append('--checks')
  print(json.dumps({'starting':name}),flush=True)
  with (job/(name+'.log')).open('w') as log:
   subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  report=json.loads((job/(name+'.json')).read_text())
  result={'name':name,'gpus':gpus,'fraction':fraction,'chunk':chunk,'status':report['status'],'cases':{k:v['medians'] for k,v in report['cases'].items()}}
  results.append(result)
  (job/'large-state.json').write_text(json.dumps(results,indent=2))
  print(json.dumps(result),flush=True)
