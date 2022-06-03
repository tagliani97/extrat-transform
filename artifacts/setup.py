from core import __version__
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

packages = ["blade"]+['blade.'+p for p in setuptools.find_packages('core', include=("*"))]

setuptools.setup(
    name="blade",
    version=__version__,
    author="Lagos",
    author_email="",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={'blade': "core"},
    packages=packages,
    install_requires=['dynaconf[yaml]==3.1.*'],    
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ]
)
