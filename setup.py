import importlib
from setuptools import setup, find_packages


def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


def version():
    builder = importlib.import_module('dataset_builder')
    return builder.__version__


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name='dataset_builder',
    version=version(),
    author='Mirko Mälicke',
    author_email='mirko@hydrocode.de',
    description='Dataset builder for CAMELS-DE dataset',
    long_description=readme(),
    long_description_content_type='text/markdown',
    install_requires=requirements(),
    license='GNU General Public License v3.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['dataset-builder=dataset_builder:run']
    }
)