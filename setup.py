import re
import ast
from setuptools import setup


_version_re = re.compile(r'__version__\s+=\s+(.*)')


with open('prestodb/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


setup(
    name='presto-python-client',
    author='Presto Team',
    author_email='presto-users@googlegroups.com',
    version=version,
    url='http://github.com/prestodb/presto-python-client',
    packages=['prestodb'],
    description='Client for the Presto distributed SQL Engine',
    classifiers=[
        'License :: OSI Approved :: Apache License 2.0',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Database :: Front-Ends',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
