### UNIT TESTS

#%%
import pytest

import numpy as np 
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src/stz")
from features import *

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from conftests import FeaturesTestingData

df = FeaturesTestingData()

#%% TEST FUNC
def test_missing_val_handler(df=df):

    test_1 = missing_val_handler(df['Nulls_actual'])

    actual_set =  set(test_1 )
    expected_set = set(df['Nulls_expected'])
    
    message = ("\nmissing_val_handler(null_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###
    ###

#%% TEsT FUNC
def test_clean_strs(df=df):

    # TESTING FUNCTIONS OUTCOME ACCURACY
    test_1 = clean_strs(df['Name_actual'])
    test_2 = clean_strs(df['Address_actual'])

    actual_set_test_1 =  set(test_1)
    expected_set_test_1 = set(df['Name_expected'])

    actual_set_test_2 =  set(test_2)
    expected_set_test_2 = set(df['Address_expected'])
    
    message_1 = ("\nclean_strs(name_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_1.difference(expected_set_test_1)), sorted(expected_set_test_1.difference(actual_set_test_1))))
    message_2 = ("\nclean_strs(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_2.difference(expected_set_test_2)), sorted(expected_set_test_2.difference(actual_set_test_2))))

    assert actual_set_test_1 == expected_set_test_1, print(message_1) # for formatting to work, message must be printed not just returned
    assert actual_set_test_2 == expected_set_test_2, print(message_2) 

    # TESTING DATA STUCTURE INPUT 
    test_3 = clean_strs(['  i hope this . works!'])
    expected_3 = pd.Series('I HOPE THIS WORKS!')

    message_3 = ("\nclean_strs(list_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(test_3, expected_3))

    assert test_3[0] == expected_3[0], message_3
    ###

#%%
def test_validate_attribute(df=df):

    test_1 = validate_attribute(df['Nulls_expected'])

    actual_set =  set(test_1)
    expected_set = set([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    message = ("\nmissing_val_handler(null_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set, expected_set))

    assert actual_set == expected_set, print(message)
    ###

#%% TESTS
test_missing_val_handler()
test_clean_strs()
test_validate_attribute()
print("Feature Tests Complete!")
# %%
