#%%
from setuptools import setup, find_packages
import pathlib
###

#%% get windows path for project 
here = pathlib.Path(__file__).parent.resolve()
###

#%%
long_description = (here / "README.md").read_text(encoding="utf-8")
###

#%%

setup(
    name="AttributeBossPySpark",
    version = "0.0.4",
    include_package_data=True,
    description = "This package aims to allow for quick and basic standardization, extraction, and validation of key attributes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = "BDRUMMOND",
    author_email = "b3drumms@gmail.com",
    packages = find_packages(),
    install_requires = ['pandas', 'numpy', 'regex', 'pyspark==3.4.0', 'pathlib', 'py4j', 'PyJWT', 'findspark'],
    python_requires = '>=3.8',
     extras_require={"tests": ["pytest", "pytest_spark"]})

# %%
