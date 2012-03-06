__author__="Reinis Ivanovs <reinis.ivanovs@accenture.com>"
__date__ ="$May 20, 2011 2:40:47 PM$"

from setuptools import setup,find_packages

def debpkgver(changelog = "debian/changelog"):
    return open(changelog).readline().split(' ')[1][1:-1]

setup (
  name = '_bugzillametrics',
  version = debpkgver(),
  packages = find_packages(),

  # Declare your packages' dependencies here, for eg:
  install_requires=['tables', 'sqlalchemy', 'mysqldb'],

  # Fill in these to make your Egg ready for upload to
  # PyPI
  author = 'Reinis Ivanovs',
  author_email = 'reinis.ivanovs@accenture.com',

  description = 'Calculate statistics on bugzilla bugs',
  license = 'GPL',
  long_description= '''Fetches data from bugzilla database, calculate statistics 
  (such as bug lifecycle, inflow, outflow, active, open, resolved, etc...  bugs and formats them''',
  py_modules=["bugzillametrics/%s" % x for x in ["bugzillametrics", "cache", "config",  "count", "dump", "rebuild", "__init__"]],

  # could also include long_description, download_url, classifiers, etc.

  classifiers=[
      "Development Status :: 4 - Beta",
      "Operating System :: Unix",
      "License :: OSI Approved :: GNU General Public License (GPL)",
      "Intended Audience :: System Administrators",
      "Programming Language :: Python",
      "Topic :: Tools :: Bugzilla"
  ]
  
)
