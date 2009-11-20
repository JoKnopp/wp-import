# -*- coding: UTF-8 -*-
#!/usr/bin/env python

import os

from distutils.core import setup

setup(name='wp-import',
      version='0.2a',
      description='Wikipedia database dump importer',
      author='Wolodja Wentland',
      author_email='wentland@cl.uni-heidelberg.de',
      url='http://github.com/babilen/wp-import',
      license='GPLv3',
      scripts=['scripts/wp-import'],
      long_description = open('doc/description.rst').read(),
      packages=['wp_import'],
      package_dir={'': 'lib'},
      data_files=[
          ('share/doc/wp-import/examples/', ['examples/wpimportrc.sample']),
      ],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Science/Research',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.6',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
      ]
     )
