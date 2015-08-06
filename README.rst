PyWebHDFS
=========

|Build Status|

PyWebHDFS is a Python wrapper for the Hadoop WebHDFS REST API.

Many of the current Python HDFS clients rely on Hadoop Streaming which
requires Java to be installed on the local machine. The other option for
interacting with HDFS is to use the WebHDFS REST API. The purpose of
this project is to simplify interactions with the WebHDFS API. The
PyWebHdfs client will implement the exact functions available in the
WebHDFS REST API and behave in a manner consistent with the API.

::

    $ pip install pywebhdfs

The initial release provides for basic WebHDFS file and directory
operations including:

#. Create and Write to a File
#. Append to a File
#. Open and Read a File
#. Make a Directory
#. Rename a File/Directory
#. Delete a File/Directory
#. Status of a File/Directory
#. Checksum of a File
#. List a Directory
#. Get/Set/List/Delete Extended Attributes (Requires Hadoop 2.5.x+)

The documentation for the Hadoop WebHDFS REST API can be found at
`http://hadoop.apache.org/docs/r1.0.4/webhdfs.html`_

Pypi package: `https://pypi.python.org/pypi/pywebhdfs`_

Documentation: `http://pythonhosted.org/pywebhdfs/`_

.. _`http://hadoop.apache.org/docs/r1.0.4/webhdfs.html`: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
.. _`https://pypi.python.org/pypi/pywebhdfs`: https://pypi.python.org/pypi/pywebhdfs
.. _`http://pythonhosted.org/pywebhdfs/`: http://pythonhosted.org/pywebhdfs/

.. |Build Status| image:: https://travis-ci.org/pywebhdfs/pywebhdfs.svg?branch=master
   :target: https://travis-ci.org/pywebhdfs/pywebhdfs
