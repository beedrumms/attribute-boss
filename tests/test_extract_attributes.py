### UNIT TESTS
#%% IMPORT PACKAGES
import pytest
import pytest_spark
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from extract_attributes import Extract
Extract = Extract()

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from conftests import ExtractTestingData

df = ExtractTestingData()
###

#%% 
def test_extract_address(df=df):

    test_1 = Extract.extract_address(df['actual'])
    
    df = df.withColumn('Address_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Address_actual'])
    expected_set = set(pd_df['Address_expected'])
    
    message = ("\nextract_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###
    ###

#%%
def test_extract_postal_code(df=df):

    test_1 = Extract.extract_postal_code(df['actual'])
    
    df = df.withColumn('Postal_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Postal_actual'])
    expected_set = set(pd_df['Postal_expected'])
    
    message = ("\nextract_postal_code(Postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%%
def test_extract_phone_number(df=df):

    test_1 = Extract.extract_phone_number(df['actual'])
    
    df = df.withColumn('Phone_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Phone_actual'])
    expected_set = set(pd_df['Phone_expected'])
    
    message = ("\nextract_phone_number(phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%% TESTS
test_extract_address()
test_extract_postal_code()
test_extract_phone_number()
# %%
