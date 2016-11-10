from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future import standard_library
standard_library.install_aliases()

import logging
import os
import os.path
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import pytest


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

PRESTO_VERSION = os.environ.get('PRESTO_PYTHON_CLIENT_PRESTO_VERSION', '0.157')
PRESTO_URL_FORMAT = 'https://repo1.maven.org/maven2/com/facebook/presto/presto-server/{v}/presto-server-{v}.tar.gz'.format(v=PRESTO_VERSION)  # NOQA
TEMPDIR_PREFIX = 'tests-presto-python-client.'
# Set the environment variable below to cache Presto tarballs.
# Beware of security issues when using files from /tmp (or other shared
# directories) as someone else can leave there a modified version of Presto.
PRESTO_PYTHON_CLIENT_TEST_CACHE_DIR = os.environ.get(
    'PRESTO_PYTHON_CLIENT_TEST_CACHE_DIR',
)


def get_local_port():
    sk = socket.socket()
    sk.bind(('', 0))
    port = sk.getsockname()[1]
    sk.close()
    return port

PRESTO_HOST = 'localhost'
PRESTO_PORT = get_local_port()

JVM_CONFIG = """-server
-Xmx16G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
"""

CONFIG_PROPERTIES = """coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port={port}
query.max-memory=4GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://{host}:{port}
""".format(host=PRESTO_HOST, port=PRESTO_PORT)

NODE_PROPERTIES = """node.environment=test
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir={path}
"""


def makedirs(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if err.args == (17, 'File exists'):
            pass
        else:
            raise


def download_presto_pkg(path, version=PRESTO_VERSION):
    url = PRESTO_URL_FORMAT.format(version)
    subprocess.check_call(['curl', '-o', path, url])


def untar_presto_pkg(tarball_path):
    subprocess.check_call(['tar', 'zxf', tarball_path])


def configure_presto(config_dir, data_dir):
    jvm_path = os.path.join(config_dir, 'jvm.config')
    with open(jvm_path, 'w') as jvm_file:
        jvm_file.write(JVM_CONFIG)

    config_path = os.path.join(config_dir, 'config.properties')
    with open(config_path, 'w') as config_file:
        config_file.write(CONFIG_PROPERTIES)

    node_path = os.path.join(config_dir, 'node.properties')
    with open(node_path, 'w') as nodes_file:
        nodes_file.write(NODE_PROPERTIES.format(path=data_dir))


def install_presto(dirpath, version=PRESTO_VERSION):
    logger.info('installing Presto in %s', dirpath)
    os.chdir(dirpath)
    if PRESTO_PYTHON_CLIENT_TEST_CACHE_DIR:
        pkg_path = os.path.join(
            PRESTO_PYTHON_CLIENT_TEST_CACHE_DIR,
            'presto-server-{}.tar.gz'.format(version),
        )
    else:
        pkg_path = os.path.join(
            dirpath,
            'presto-server-{}.tar.gz'.format(version),
        )
    if not os.path.exists(pkg_path):
        pkg_dir = os.path.dirname(pkg_path)
        makedirs(pkg_dir)
        logger.info('downloading Presto package to %s', pkg_path)
        download_presto_pkg(pkg_path, version)
    else:
        logger.info('using cached Presto package at %s', pkg_path)

    untar_presto_pkg(pkg_path)
    presto_dir = os.path.basename(pkg_path).replace('.tar.gz', '')
    config_dir = os.path.join(presto_dir, 'etc')
    os.mkdir(config_dir)
    data_dir = os.path.join(presto_dir, 'data')
    configure_presto(config_dir, data_dir)
    return presto_dir


def start_presto(dirpath):
    # str -> subprocess.Popen
    logger.info('starting Presto on {}:{}'.format(PRESTO_HOST, PRESTO_PORT))
    presto_launcher = os.path.join(dirpath, 'bin', 'launcher.py')
    return subprocess.Popen(
        ['python2.7', presto_launcher, 'run'],
        universal_newlines=True,
        bufsize=1,
        stderr=subprocess.PIPE,
    )


def stop_presto(proc):
    # subprocess.Popen -> None
    os.kill(proc.pid, signal.SIGTERM)


def wait_for_presto(stream):
    started_tag = '======== SERVER STARTED ========'
    for line in stream:
        logger.info('DEBUG %s', line)
        if started_tag in line:
            logger.info('Presto has started')
            break


@pytest.fixture(scope='module')
def run_presto():
    dirpath = tempfile.mkdtemp(prefix=TEMPDIR_PREFIX)
    presto_dir = install_presto(dirpath)
    proc = start_presto(presto_dir)
    wait_for_presto(proc.stderr)
    yield
    assert bool(TEMPDIR_PREFIX)
    assert TEMPDIR_PREFIX in dirpath
    logger.info('deleting directory %s', dirpath)
    shutil.rmtree(dirpath)
    stop_presto(proc)
