# AttributeBoss

<p align="center">
<img src="https://github.com/beedrumms/attribute-boss/blob/bc755f2f95494ea0df7fe2ddf89be0140df8bf9d/assets/ab.png" alt="AttributeBoss Logo" height="300px">
</p>

# Routine Data Standardization
AttributeBoss performs routine standardization and extraction tasks common to data transformation preprocessing requirements. 

## This library can:
   - standardize the treatment of missing data (`missing_val_handler()`)
   - clean any str data (`str_prep()`)
   - validate address, postal, and phone data (`validate_attribute()`)
   - standardize address data by removing aligning use of prefix and suffixes and street directions (`standardize_address()`)
   - standardize postal codes by removing common differences in representation (`standardize_postal_code()`)
   - standardize phone numbers by removing common differences in reporesentation (`standardize_phone_number()`)
   - standardize province and state names to abbreviated title (`standardize_province_state()`)
   - extract an address (street number, street name, street prefix / suffix, street directions) from a complex str (`extract_address()`)
   - extract a postal code from a str (`extract_postal_code()`)
   - extract a phone number from a str (`extract_phone_number()`)

**see Suggested Steps for Implementation below for proper use**
   
AttributeBoss Forever 

Signing off but inevitably back on 
-B 

# How to Use
- This library was built to quickly bring key attributes into alignment (Standardize) for further preprocessing tasks 
- It has Spark and Python functionality. 

## Spark
`from AttributeBoss.SparkTools import SparkBoss`
`ab = SparkBoss()`
- The Spark compatibilities of AttributeBoss use PySpark dataframe column instances to transform data. 
- To initialize:
   

## Python
`from AttributeBoss.PyTools import PyBoss`
`ab = PyBoss()`
- The base python functions in AttributeBoss are built around the **assumption that Pandas DataFrames are being used**, and they are therefore required input types. 
- most of the functions leverage py dict structures to perform replacement tasks given that they are much more efficient than using for loops or step by step tasks
- with this in mind, the **input for each function should be a pd.Series** unless otherwise stated
- the output of **each function is a pd.Series**, which will make it easy to write it back to your DF
- ex:
   - `standardized_attribute = standardize_address(df['my_attribute'])`
   - `df['my_attribute] = standardized_attribute` 


# Suggested Steps for Implementation 
- this library was designed to be used in steps and **some functions rely on others to work properly**
- It is suggested that a user take the following approach:
   - for **address data**: missing_val_handler() --> standardize_address() --> extract_address() --> validate_attribute()
      - standardize_address() leverages cln_str() so it is not neccessary to use it 
      - extract_address() is looking for a matches based off the standard prefix / suffix / street direction abbreviations implemented via standardize_address 
      - validate_attribute() is looking only for NoneType data and returning a list of indices that match None -- in this way, extract_address() is actually validating your address data and returning None where it doesnt find a match. 
   - **any string**: missing_val_handler() --> cln_strs() 
      - cln_strs() performs basic cleaning in this order (the order is important): Changes str to uppercase, removes new lines, different types of apostrophies, periods, hyphens, parenthesis, adjusts commas. Replaces all types of french accents with standard letter. Removes all non-ascii characters, and finally remove any spaces more than one and trim any remaining whitespaces.
      - note that cln_strs() should be satisfactory for most categorical data -- however, it is not intended for text based data where puncuation matters! 
   - **postal, phone, or province data** missing_val_handler() --> standardize_xxx() --> extract_xxx() --> validate_attribute()





