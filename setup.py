try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

config = {
    'description': 'BioMAJ Migration tool',
    'author': 'Olivier Sallou',
    'url': 'http://biomaj.genouest.org',
    'download_url': 'http://biomaj.genouest.org',
    'author_email': 'olivier.sallou@irisa.fr',
    'version': '3.0.2',
    'install_requires': ['nose',
                         'mysql-connector-python-rf',
                         'pymongo'],
    'packages': find_packages(),
    'include_package_data': True,
    'scripts': ['bin/biomaj-migrate.py'],
    'name': 'biomajmigrate'
}

setup(**config)
