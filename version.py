""" Module specifically to hold version information.  The reason this
exists is the version information is needed in setup.py for install.
If these values were defined in change_worker/__init__.py then install
would fail because there are other dependencies imported in
change_worker/__init__.py that are not present until after
install. Do not import anything into this module."""
__version__ = '0.1.0'
__name = 'lcmap-change-worker'
__algorithm__ = '-'.join([__name, __version__])
