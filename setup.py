from setuptools import setup, find_packages


def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(
    name='harvest',
    version='0.1.0',
    install_requires=requirements(),
    license='GNU General Public License v3.0',
    packages=find_packages(),
)