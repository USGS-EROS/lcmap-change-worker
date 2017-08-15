""" Module specifically to hold version information.  The reason this
exists is the version information is needed in setup.py for install.
If these values were defined in pw/__init__.py then install
would fail because there are other dependencies imported in
pw/__init__.py that are not present until after
install. Do not import anything into this module."""

""" __version__ MUST align with desired lcmap-pyccd version"""
__pyccd_version__ = '2017.6.20'
__version__ = '2017.8.15'
__name = 'lcmap-pyccd-worker'
__algorithm__ = '-'.join([__name, __pyccd_version__])
