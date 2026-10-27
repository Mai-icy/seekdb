from pathlib import Path
import hashlib,json,os,subprocess,time,signal
base=Path('/data/wuhao.xwh/seekdb-sirius-20261022')
repo=base/'seekdb'
job=Path('/data/wuhao.xwh/seekdb-sirius-multigpu-20261027')
pixi=Path('/data/wuhao.xwh/duckdb-sirius-upstream/sirius/.pixi/envs/default')
previous=int((job/'large.pid').read_text())
while Path(f'/proc/{previous}/cmdline').exists() and b'run-large.py' in Path(f'/proc/{previous}/cmdline').read_bytes():
 time.sleep(5)
state=json.loads((job/'large-state.json').read_text())
assert len(state)==8 and all(r['status']=='passed' for r in state), 'Large matrix incomplete'
env=dict(os.environ,CUDA_VISIBLE_DEVICES='0,1',SEEKDB_PORT='2886',SEEKDB_DATABASE='test',SEEKDB_SIRIUS_CONFIG=str(job/'g2-0.075.yaml'),SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB='2048',LD_LIBRARY_PATH=f'{job}/overlay:{pixi}/lib',PYTHONPATH=str(pixi/'lib/python3.14/site-packages'))
path=Path('/data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock')
assert not path.exists()
with (job/'cli-sidecar.log').open('w') as log:
 sidecar=subprocess.Popen([str(base/'venv/bin/python'),str(repo/'tools/sirius/sidecar.py'),'--socket',str(path),'--library',str(job/'bridge/libseekdb_sirius_bridge.so'),'--host-pin-mib','64','--native-prefetch','--native-chunk-mib','1536','--warmup'],cwd=job,env=env,stdout=log,stderr=subprocess.STDOUT)
 try:
  deadline=time.monotonic()+60
  while not path.exists():
   if sidecar.poll() is not None or time.monotonic()>deadline: raise RuntimeError('Sidecar startup failed')
   time.sleep(.1)
  for name,args in [('cli-live',['live_integration.py','--socket',str(path)]),('cli-native-cancel',['prefetch_integration.py','--oracle','/data/wuhao.xwh/seekdb-sirius-compare-20261026/oracle.json']),('proxy-tests',['test_sidecar_transport.py','--library',str(repo/'build-sirius-arrow/libseekdb_sirius_proxy.so')])]:
   with (job/(name+'.log')).open('w') as output:
    subprocess.run([str(base/'venv/bin/python'),str(repo/'unittest/sql/sirius'/args[0]),*args[1:]],env=env,cwd=job,stdout=output,stderr=subprocess.STDOUT,check=True,timeout=120)
 finally:
  if sidecar.poll() is None:
   sidecar.terminate()
   try: sidecar.wait(timeout=65)
   except subprocess.TimeoutExpired: sidecar.kill();sidecar.wait();raise
assert sidecar.returncode==0 and not path.exists()
source=Path('/data/wuhao.xwh/duckdb-sirius-data/tpch_sf500.duckdb')
assert (source.stat().st_size,source.stat().st_mtime_ns)==(140138786816,1791462173336194573)
borrowed=Path('/data/wuhao.xwh/duckdb-sirius-upstream/sirius')
assert not subprocess.check_output(['git','-C',str(borrowed),'status','--porcelain'])
original=borrowed/'build/release/extension/sirius/libsirius.so'
with original.open('rb') as stream: original_sha=hashlib.file_digest(stream,'sha256').hexdigest()
assert original_sha=='cf5c490ac32f3cf7169bea8011688eedaefb634ee53741a3f475b7d729bd78ae'
generated=repo/'unittest/sql/sirius/telemetry_data'
if generated.exists(): generated.rename(job/'unit-test-telemetry')
report={'status':'passed','cli_sidecar_pid':sidecar.pid,'cli_exit':sidecar.returncode,'socket_removed':True,'source_unchanged':True,'source_bytes':source.stat().st_size,'source_mtime_ns':source.stat().st_mtime_ns,'borrowed_sirius_clean':True,'borrowed_library_sha256':original_sha,'gpu_processes_after':subprocess.check_output(['nvidia-smi','--query-compute-apps=gpu_uuid,pid,used_gpu_memory','--format=csv'],text=True),'disk_after':subprocess.check_output(['df','-h','/data'],text=True)}
(job/'cleanup.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report),flush=True)
