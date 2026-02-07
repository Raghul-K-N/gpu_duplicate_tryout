from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
ext_modules = [
    #Extension("Data_preparation",  ["Data_preparation.py"]),
    Extension("db_connector",  ["db_connector.py"]),
    Extension("IForest",  ["IForest.py"]),
    Extension("Rules",  ["Rules.py"]),
    Extension("SHAP",  ["SHAP.py"])
]
setup(
    name = 'ML',
    cmdclass = {'build_ext': build_ext},
    ext_modules = ext_modules
)