### UNIT TESTS

#%% IMPORT PACKAGES
import pytest
import pytest_spark
import pandas as pd

import os
import sys
#sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
#from spark_setup import MySparkSession

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from features import missing_val_handler, clean_strs

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from conftests import FeaturesTestingData

df = FeaturesTestingData()
###

#%% TEST missing_val_handler
def test_missing_val_handler(df=df):

    test_1 = missing_val_handler(df['Nulls_actual'])
    
    df = df.withColumn('Nulls_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Nulls_actual'])
    expected_set = set(pd_df['Nulls_expected'])
    
    message = ("\nmissing_val_handler(null_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###

#%% TEsT FUNC
def test_clean_strs(df=df):

    test_1 = clean_strs(df['Name_actual'], include_accents=True)
    test_2 = clean_strs(df['Address_actual'])
    
    df = df.withColumn('Name_actual', test_1)    
    df = df.withColumn('Address_actual', test_2)    

    pd_df = df.toPandas()

    actual_set_test_1 =  set(pd_df['Name_actual'])
    expected_set_test_1 = set(pd_df['Name_expected'])

    actual_set_test_2 =  set(pd_df['Address_actual'])
    expected_set_test_2 = set(pd_df['Address_expected'])
    
    message_1 = ("\nclean_strs(name_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_1.difference(expected_set_test_1)), sorted(expected_set_test_1.difference(actual_set_test_1))))
    message_2 = ("\nclean_strs(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set_test_2.difference(expected_set_test_2)), sorted(expected_set_test_2.difference(actual_set_test_2))))

    assert actual_set_test_1 == expected_set_test_1, print(message_1) # for formatting to work, message must be printed not just returned
    assert actual_set_test_2 == expected_set_test_2, print(message_2) 
    ###

#%% TESTS
test_missing_val_handler()
test_clean_strs()
# %%
