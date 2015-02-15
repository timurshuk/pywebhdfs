# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

with open('requirements.txt') as f:
    dependencies = f.read().splitlines()

with open('test-requirements.txt') as f:
    test_dependencies = f.read().splitlines()

setup(
    name='pywebhdfs',
    version='0.2.3',
    description='Python wrapper for the Hadoop WebHDFS REST API',
    author='Steven D. Gonzales',
    author_email='stevendgonzales@gmail.com',
    url='https://github.com/ProjectMeniscus/pywebhdfs',
    tests_require=test_dependencies,
    install_requires=dependencies,
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup'])
)
