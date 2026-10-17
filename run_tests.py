"""No network; original suites run in disposable copies, never in the old studies."""
import hashlib,json,pathlib,shutil,subprocess,sys,tempfile
R=pathlib.Path(__file__).resolve().parent
for name,expected in json.loads((R/'VENDOR_IDENTITY.json').read_text()).items():assert hashlib.sha256((R/'vendor'/name).read_bytes()).hexdigest()==expected
with tempfile.TemporaryDirectory() as tmp:
 p=pathlib.Path(tmp)
 for old,new in [('engine.py','study.py'),('engine_tests.py','tests.py'),('engine_test_config.json','config.json')]:shutil.copyfile(R/'vendor'/old,p/new)
 subprocess.run([sys.executable,str(p/'tests.py')],check=True)
with tempfile.TemporaryDirectory() as tmp:
 p=pathlib.Path(tmp);(p/'gen').mkdir()
 for name in ['engine.py','study.py']:shutil.copyfile(R/'vendor'/name,p/'gen'/name)
 shutil.copyfile(R/'vendor/b_config.json',p/'gen/config.json');shutil.copyfile(R/'vendor/test_alerts.py',p/'test_alerts.py');subprocess.run([sys.executable,'-m','unittest','test_alerts','-v'],cwd=p,check=True)
subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=R,check=True)
files=['adapter.py','bootstrap.py','cloud.py','persistence.py','journal.py','config.json','ADMISSION_POLICY.json','run_tests.py']
(R/'TEST_RESULTS.json').write_text(json.dumps({'success':True,'scope':'Original engine and B suites plus corrected-data regression/persistence tests; no edge claim','hashes':{n:hashlib.sha256((R/n).read_bytes()).hexdigest() for n in files}},indent=2)+'\n')
