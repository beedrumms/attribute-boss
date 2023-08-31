### UNIT TESTS

#%%
import pytest

import numpy as np 
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src/stz")
from standardize_attributes import Standardize
Stz = Standardize()

sys.path.append(os.path.dirname(os.path.realpath(__file__))) 
from conftests import StandardizeTestingData 
df = StandardizeTestingData()

#%% TEST FUNC
def test_standardize_address(df=df):
    
    test_1 = Stz.standardize_address(df['Address_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Address_expected'])
    
    message = ("\nstandardize_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%% TEsT FUNC
def test_standardize_postal_code(df=df):

    test_1 = Stz.standardize_postal_code(df['Postal_actual'])
    
    actual_set =  set(test_1)
    expected_set = set(df['Postal_expected'])
    
    message = ("\nstandardize_postal_code(postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###

#%%
def test_standardize_phone_number(df=df):

    test_1 = Stz.standardize_phone_number(df['Phone_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Phone_expected'])
    
    message = ("\nstandardize_phone_number(Phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###

#%%
def test_standardize_province_state(df=df):

    test_1 = Stz.standardize_province_state(df['Prov_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Prov_expected'])
    
    message = ("\nstandardize_province_state(Prov_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


#%% TESTS
test_standardize_address()
test_standardize_postal_code()
test_standardize_phone_number()
test_standardize_province_state()
print("Standardize tests are complete!")
# %%
