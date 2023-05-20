### UNIT TESTS

#%%
import pytest
import importlib.util

## ABSOLUTE and RELATIVE IMPORTS NOT WORKING FOR LOCAL PACKAGES
# from attributeboss.extract_attributes import Extract 

### TEMP SOLUTION FOR TESTING
temp_path = 'c:/Users/DrummoBre/OneDrive - Government of Ontario/Desktop/SAMS/projects/py_packages/package_zone/AttributeBoss/src/attributeboss/extract_attributes.py'
# specify the module that needs to be imported relative to the path of the module
spec = importlib.util.spec_from_file_location("extract_attributes", temp_path)
# create a new module based on spec
foo = importlib.util.module_from_spec(spec)
# executes module in its own namespace when modeul is imported or reloaded
spec.loader.exec_module(foo)
## Initialize Class in this scope
Extract = foo.Extract()

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
test_extract_postalcode()
# %%
