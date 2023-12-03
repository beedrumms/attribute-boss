#%% 

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, trim, upper

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from SparkTools import SparkBoss
boss = SparkBoss()


sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from cfgtests_spark import MyTestingSparkSession, FeaturesTestingData, StandardizeTestingData, ExtractTestingData

import regex as re
#### 
spark = MyTestingSparkSession()
df_features = FeaturesTestingData(spark)
df_standardize = StandardizeTestingData(spark)
df_extract = ExtractTestingData(spark)


#%% 
def test_missing_val_handler(df=df_features):

    test_1 = boss.missing_val_handler(df['Nulls_actual'])
    
    df = df.withColumn('Nulls_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Nulls_actual'])
    expected_set = set(pd_df['Nulls_expected'])
    
    message = ("\nmissing_val_handler(null_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


def test_str_prep(df=df_features):

    test_1 = boss.str_prep(df['Name_actual'])
    test_2 = boss.str_prep(df['Address_actual'])
    
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


def test_standardize_address(df=df_standardize):

    test_1 = boss.standardize_address(df['Address_actual'])
    
    df = df.withColumn('Address_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Address_actual'])
    expected_set = set(pd_df['Address_expected'])
    
    message = ("\nstandardize_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###
    ###

def test_standardize_postal_code(df=df_standardize):
 
    test_1 = boss.standardize_postal_code(df['Postal_actual'])
    
    df = df.withColumn('Postal_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Postal_actual'])
    expected_set = set(pd_df['Postal_expected'])
    
    message = ("\nstandardize_postal_code(postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_standardize_phone_number(df=df_standardize):

    test_1 = boss.standardize_phone_number(df['Phone_actual'])
    
    df = df.withColumn('Phone_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Phone_actual'])
    expected_set = set(pd_df['Phone_expected'])
    
    message = ("\nstandardize_phone_number(Phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_standardize_province_state(df=df_standardize):
 
    test_1 = boss.standardize_province_state(df['Prov_actual'])
    
    df = df.withColumn('Prov_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Prov_actual'])
    expected_set = set(pd_df['Prov_expected'])
    
    message = ("\nstandardize_province_state(Prov_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) 
    ###


def test_extract_address(df=df_extract):

    test_1 = boss.extract_address(df['actual'])
    
    df = df.withColumn('Address_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Address_actual'])
    expected_set = set(pd_df['Address_expected'])
    
    message = ("\nextract_address(address_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###
    ###


def test_extract_postal_code(df=df_extract):

    test_1 = boss.extract_postal_code(df['actual'])
    
    df = df.withColumn('Postal_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Postal_actual'])
    expected_set = set(pd_df['Postal_expected'])
    
    message = ("\nextract_postal_code(Postal_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(sorted(actual_set.difference(expected_set)), sorted(expected_set.difference(actual_set))))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


def test_extract_phone_number(df=df_extract):

    test_1 = boss.extract_phone_number(df['actual'])
    
    df = df.withColumn('Phone_actual', test_1)    

    pd_df = df.toPandas()

    actual_set =  set(pd_df['Phone_actual'])
    expected_set = set(pd_df['Phone_expected'])
    
    message = ("\nextract_phone_number(phone_test) RETURNED: \n{0}\n INSTEAD OF: \n{1}\n".format(actual_set.difference(expected_set), expected_set.difference(actual_set)))

    assert actual_set == expected_set, print(message) # for special formatting to work, message must be printed not just returned
    ###


#%% 
test_missing_val_handler()
test_str_prep()
print("Features tests are complete")

test_standardize_address()
test_standardize_postal_code()
test_standardize_phone_number()
test_standardize_province_state()
print("Standardized tests complete!")

test_extract_address()
test_extract_postal_code()
test_extract_phone_number()

print("Extract tests are complete")

print("tests are all complete")
