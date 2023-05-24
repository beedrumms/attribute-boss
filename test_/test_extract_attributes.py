### UNIT TESTS

#%%
import pytest
import importlib.util

import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from extract_attributes import Extract

# %%
Extract = Extract()

#%% 
def test_extract_address():

    test_item1 = ["Queen's Hospital , Suite E3 FLR 5, Attn to Dr Jarvis, 122-312 JARVIS AVE RD E, M4r 2P8, "]

    actual1 = Extract.extract_address(test_item1)  
    expected1 = ["312 JARVIS AVE RD E"]   
    message1 = ("extract_address ""returned {0} instead ""of {1}".format(actual1, expected1))

    test_item2 = [None]

    actual2 = Extract.extract_address(test_item2)  
    expected2 = [None]  
    message2 = ("extract_address ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual1 == expected1, message1
    assert actual2 == expected2, message2

#%%
def test_extract_postalcode():

    test_item = ["Queen's Hospital , Suite E3 FLR 5, Attn to Dr Jarvis, 122-312 JARVIS AVE RD E, M4R2P8, 90234"]

    actual = Extract.extract_postalcode(test_item)  
    expected = ["M4R2P8"]   
    message = ("extract_address ""returned {0} instead ""of {1}".format(actual, expected))


    test_item2 = [None] # if function is fed a None value - it is to return None

    actual2 = Extract.extract_postalcode(test_item2)  
    expected2 = [None]   
    message2 = ("extract_address ""returned {0} instead ""of {1}".format(actual2, expected2))

    assert actual == expected, message
    assert actual2 == expected2, message2


#%% TESTS
test_extract_address()

# %%
test_extract_postalcode()
# %%
