### UNIT TESTS

#%%
import pytest

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src/stz")
from extract_attributes import Extract
Extract = Extract()

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from conftests import ExtractTestingData
df = ExtractTestingData()

#%% 
def test_extract_address(df=df):

    test_1 = Extract.extract_address(df['actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Address_expected'])
    
    message = ("\nextract_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%%
def test_extract_postal_code(df=df):

    test_1 = Extract.extract_postal_code(df['actual'])
    
    actual_set =  set(test_1)
    expected_set = set(df['Postal_expected'])
    
    message = ("\nextract_postal_code(Postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%%
def test_extract_phone_number(df=df):

    test_1 = Extract.extract_phone_number(df['actual'])

    actual_set =  set(test_1)
    expected_set = set(df['Phone_expected'])
    
    message = ("\nextract_phone_number(phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%% TESTS
test_extract_address()
test_extract_postal_code()
test_extract_phone_number()
print("Extract tests are complete!")
#%%
