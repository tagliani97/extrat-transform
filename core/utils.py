import os
import sys
import tempfile

from enum import Enum
from pathlib import Path
from textwrap import wrap
from collections import namedtuple

Node = namedtuple("node",["name","value"])

init()
home = Path.home()
home_alla = os.path.join(home, ".alla")

tempdir = None

try:
    ipy_str = str(type(get_ipython()))
    if 'zmqshell' in ipy_str:
        TYPE_OF_SCRIPT = 'jupyter'

    if 'terminal' in ipy_str:
        TYPE_OF_SCRIPT = 'ipython'

except:
    TYPE_OF_SCRIPT ='terminal'

def gen_temp(*prefixs):
    """generates an address from the temporary folder
    
    Parameters
    ----------
    *prefixs: str
        one or more prefix to be added to the path

    Returns
    -------
    str
    """
    global tempdir
    if(not tempdir):
        tempdir = tempfile.mkdtemp()
    return os.path.join(tempdir, *prefixs)

def gen_path(*prefixs):
    """generates an address from the '.alla' folder within the user's home folde
    
    Parameters
    ----------
    *prefixs: str
        one or more prefix to be added to the path

    Returns
    -------
    str
    """
    prefix = os.path.join(home_alla, *prefixs)
    check_and_create_dir(os.path.dirname(prefix))
    return os.path.join(prefix)

def gen_path_inside(*prefixs):
    """generates an address from the root of the module
    
    Parameters
    ----------
    *prefixs: str
        one or more prefix to be added to the path

    Returns
    -------
    str
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), *prefixs)

def check_and_create_dir(directory):
    """checks and tries to create the directory if it does not exist
    
    Parameters
    ----------

    directory: str
        directory path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
