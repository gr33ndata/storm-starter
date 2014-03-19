import sys
import subprocess as sp 

# Check if Redis Server is running
# Putting the r of redis between brackets is
# a hack to prevent grep from grepping itself
proc = sp.Popen(
    'ps -ax | grep [r]edis-server',
    stdout=sp.PIPE,  
    stderr=sp.STDOUT,
    shell=True)

lines = list(proc.stdout)
if len(lines) != 0:
    print 'Redis Server is running [OK]'
else:
    print 'Redis Server is not running [Exiting]'
    sys.exit()

# Check if virtualenv is activated
proc = sp.Popen(
    'echo $PATH',
    stdout=sp.PIPE,  
    stderr=sp.STDOUT,
    shell=True)

path = proc.stdout.read()
venv_path = path.split(':')[0]
if venv_path.split('/')[-2] == 'venv':
    print 'Virtualenv is activated [OK]'
else:
    print 'Virtualenv is not activated [Exiting]'
    sys.exit()

# Run Maven and Apache Storm
#proc = sp.Popen(
#    'mvn -e -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dstorm.topology=storm.starter.TweetsLanguages',
#    #stdout=sp.PIPE,  
#    stderr=sp.STDOUT,
#    shell=True)
print 'All is set, now run ./run.sh'




