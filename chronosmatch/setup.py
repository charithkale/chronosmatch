"""Build the optional Cython matching engine:  python setup.py build_ext --inplace"""
from setuptools import Extension, setup

try:
    from Cython.Build import cythonize
    ext = cythonize([Extension("chronosmatch._matcher", ["chronosmatch/_matcher.pyx"],
                               extra_compile_args=["-O3", "-march=native"])],
                    compiler_directives={"language_level": 3})
except ImportError:
    ext = []        # pure-Python fallback is used automatically

setup(name="chronosmatch", version="1.0.0", packages=["chronosmatch"], ext_modules=ext)
