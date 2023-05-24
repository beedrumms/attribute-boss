### UNIT TESTS

#%%
import pytest
import importlib.util
import numpy as np 
import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from utils import *

#%% TEST FUNC
def test_missing_val_handler():

    test_item = [None, np.nan, "", '', "None", "nA", "nan", "YAY"]

    actual = missing_val_handler(test_item)  
    expected = pd.Series([None, None, None, None, None, None, None, "YAY"])  
    message = ("missing_val_handler ""returned {0} instead ""of {1}".format(actual, expected))

    assert set(actual) == set(expected), message
    ###

#%% TEsT FUNC
def test_clean_str():

    test_item1 = r"""Queen's Hospital   Suite E3 FLR 5, Attn to Dr    Jarvis, 122-312
                    JARVIS AVE RD E, M4r 2P8, """
    actual1 = clean_str(test_item1)  
    expected1 = r"QUEENS HOSPITAL SUITE E3 FLR 5 ATTN TO DR JARVIS 122-312 JARVIS AVE RD E M4R 2P8"   
    message1 = ("clean_str ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = None
    actual2 = clean_str(test_item2)  
    expected2 = None
    message2 = ("clean_str ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2
    ###

#%%
def test_validate_attribute():

    test_item = ["I", None, "hope", None, "This", None, "Works"]

    actual = validate_attribute(test_item)  
    expected = [1, 3, 5]   
    message = ("validate_address ""returned {0} instead ""of {1}".format(actual, expected))

    assert set(actual) == set(expected), message
    ###

#%% TESTS
test_missing_val_handler()
test_clean_str()
test_validate_attribute()
# %%
