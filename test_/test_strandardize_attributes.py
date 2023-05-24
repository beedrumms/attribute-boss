### UNIT TESTS

#%%
import pytest
import importlib.util
import numpy as np 
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from standardize_attributes import Standardize
Stz = Standardize()


#%% TEST FUNC
def test_standardize_address():

    test_item = "Queen's Hospital , Suite E3 FLR   5,   Attn to Dr Jarvis, 122-312 JARVIS AVENUE RoaD East, M4r 2P8, "

    actual1 = Stz.standardize_address(test_item)  
    expected1 = "QUEENS HOSPITAL SUITE E3 FLR 5 ATTN TO DR JARVIS 122-312 JARVIS AVE RD E M4R 2P8"   
    message1 = ("standardize_address ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = None

    actual2 = Stz.standardize_address(test_item2)  
    expected2 = None  
    message2 = ("standardize_address ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2
    ###

#%% TEsT FUNC
def test_standardize_postal_code():

    test_item1 = r"""Queen's Hospital   Suite E3 FLR 5, Attn to Dr    Jarvis, 122-312
                    JARVIS AVE RD E, M4r 2P8, """
    actual1 = Stz.standardize_postal_code(test_item1)  
    expected1 = r"""Queen's Hospital   Suite E3 FLR 5, Attn to Dr    Jarvis, 122-312
                    JARVIS AVE RD E, M4R2P8, """   
    message1 = ("standardize_postal_code ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = None
    actual2 = Stz.standardize_postal_code(test_item2)  
    expected2 = None
    message2 = ("standardize_postal_code ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2
    ###

#%%
def test_standardize_phone_number():

    test_item1 = r"910- 345. 1023  "
    actual1 = Stz.standardize_phone_number(test_item1)  
    expected1 = r"9103451023"   
    message1 = ("standardize_phone_number ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = None
    actual2 = Stz.standardize_phone_number(test_item2)  
    expected2 = None
    message2 = ("standardize_phone_number ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2
    ###

#%%
def test_standardize_province_state():

    test_item1 = r"ALABAMA  ALASKA  WISCONSIN   WASHINGTON "
    actual1 = Stz.standardize_province_state(test_item1)  
    expected1 = r"AL AK WI WA"   
    message1 = ("standardize_prov_state ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = None
    actual2 = Stz.standardize_province_state(test_item2)  
    expected2 = None
    message2 = ("standardize_prov_state ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2
    ###

#%% TESTS
test_standardize_address()
test_standardize_postal_code()
test_standardize_phone_number()
test_standardize_province_state()
# %%
