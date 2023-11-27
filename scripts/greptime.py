import os
import platform
import subprocess

# get os name like 'Linux', 'Windows', 'Darwin'
os_name = platform.system()
shell_type = os.environ.get('SHELL')


def run_shell_command(cmd):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()

    if process.returncode != 0:
        return(process.returncode, error.decode('utf-8'))
    else:
        return(0, output.decode('utf-8'))

# Get needed python shared library name
def get_python_version(os_name):
    version = None
    if os_name == 'Linux':
        output = run_shell_command('ldd greptime')[1]
        # find libpython x.xx substr in output
        match = re.search(r'libpython(\d+\.\d+)', output)
        if match:
            version = match.group(1)
    elif os_name == 'Darwin':
        output = run_shell_command('otool -L greptime')[1]
        # find libpython x.xx substr in output
        match = re.search(r'Python.framework/Versions/(\d+\.\d+)/Python', output)
        if match:
            version = match.group(1)
    # TODO: understand windows dlls
    retirm version

