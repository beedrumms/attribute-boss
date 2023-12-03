### UNIT TESTS

#%%
import pytest

import numpy as np 
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from AttributeBoss.PyTools import PyBoss # as boss
boss = PyBoss()

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from cfgtests_py import FeaturesTestingData, StandardizeTestingData, ExtractTestingData

df_features = FeaturesTestingData()
df_standardize = StandardizeTestingData()
df_extract = ExtractTestingData()


#%%
def test_missing_val_handler(df=df_features):

    test_1 = boss.missing_val_handler(df['Nulls_actual'])

    actual_set =  set(test_1 )
    expected_set = set(df['Nulls_expected'])
    
    message = ("\nmissing_val_handler(null_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###
    ###


def test_str_prep(df=df_features):

    # TESTING FUNCTIONS OUTCOME ACCURACY
    test_1 = boss.str_prep(df['Name_actual'])
    test_2 = boss.str_prep(df['Address_actual'])

    actual_set_test_1 =  set(test_1)
    expected_set_test_1 = set(df['Name_expected'])

    actual_set_test_2 =  set(test_2)
    expected_set_test_2 = set(df['Address_expected'])
    
    message_1 = ("\nclean_strs(name_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_1.difference(expected_set_test_1)), sorted(expected_set_test_1.difference(actual_set_test_1))))
    message_2 = ("\nclean_strs(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_2.difference(expected_set_test_2)), sorted(expected_set_test_2.difference(actual_set_test_2))))

    assert actual_set_test_1 == expected_set_test_1, print(message_1) # for formatting to work, message must be printed not just returned
    assert actual_set_test_2 == expected_set_test_2, print(message_2) 

    # TESTING DATA STUCTURE INPUT 
    test_3 = boss.str_prep(['  i hope this . works!'])
    expected_3 = pd.Series('I HOPE THIS WORKS!')

    message_3 = ("\nclean_strs(list_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(test_3, expected_3))

    assert test_3[0] == expected_3[0], message_3
    ###


def test_standardize_address(df=df_standardize):
    
    test_1 = boss.standardize_address(df['Address_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Address_expected'])
    
    message = ("\nstandardize_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


def test_standardize_postal_code(df=df_standardize):

    test_1 = boss.standardize_postal_code(df['Postal_actual'])
    
    actual_set =  set(test_1)
    expected_set = set(df['Postal_expected'])
    
    message = ("\nstandardize_postal_code(postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_standardize_phone_number(df=df_standardize):

    test_1 = boss.standardize_phone_number(df['Phone_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Phone_expected'])
    
    message = ("\nstandardize_phone_number(Phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_standardize_province_state(df=df_standardize):

    test_1 = boss.standardize_province_state(df['Prov_actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Prov_expected'])
    
    message = ("\nstandardize_province_state(Prov_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_extract_address(df=df_extract):

    test_1 = boss.extract_address(df['actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Address_expected'])
    
    message = ("\nextract_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


def test_extract_postal_code(df=df_extract):

    test_1 = boss.extract_postal_code(df['actual'])
    
    actual_set =  set(test_1)
    expected_set = set(df['Postal_expected'])
    
    message = ("\nextract_postal_code(Postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


def test_extract_phone_number(df=df_extract):

    test_1 = boss.extract_phone_number(df['actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Phone_expected'])
    
    message = ("\nextract_phone_number(phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%% 

test_missing_val_handler()
test_str_prep()
print("Feature Tests Complete!")

test_standardize_address()
test_standardize_postal_code()
test_standardize_phone_number()
test_standardize_province_state()
print("Standardize tests are complete!")


test_extract_address()
test_extract_postal_code()
test_extract_phone_number()
print("Extract tests are complete!")

print("All tests are complete")
