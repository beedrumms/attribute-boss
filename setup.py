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
    name="AttributeBoss",
    version = '0.0.1',
    description = "This package aims to allow for quick and basic standardization, extraction, and validation of key attributes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = "BDRUMMOND",
    author_email = "b3drumms@gmail.com",
    package_dir={"": "src"},
    packages = find_packages(where="src", exclude=['tests']),
    install_requires = ['pandas', 'numpy', 'regex'],
     extras_require={"tests": ["pytest"]})

# %%
