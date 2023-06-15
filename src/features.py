#%% IMPORT PACKAGES
# basic py packages
import pandas as pd
import numpy as np

# basic py packages
import sys
import os

import pyspark
from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, trim, upper

###

#%% FUNC 1 
def missing_val_handler(your_col_inst=None):
        
        """
        replaces every missing value ("", None, or np.nan) or value labeled missing (NA, NONE, or NAN) to None value type 
        
        ARGS
        your_col_inst (col_instance) your attribute list that you want to standardize missing vals
        
        RETURNS
        a col object of your attribute with missing values standarized as None 

        NOTES
        col_instance = df[col_name] 
        
        To use function: `df = df.withColumn(col_name, missing_val_handler(your_col_inst))` 
        
        """
        try:
            updated_col_inst = when(your_col_inst.isin(["NA", "Na", "nA", "na", "", np.nan, "NONE", "none", "None", "NAN", "NaN", "nan"]), None).otherwise(your_col_inst)

            return updated_col_inst

        except AttributeError as e:
            
            print("Missing required argument or incorrect data struct given for arg *col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
            raise

#%%
def clean_strs(your_col_inst=None, include_accents=False):

        """
        Converts str to upper, removes any double+ spaces, apostrophes, periods, hyphens, parentheses, non-ASCII characters, strips trailing ws, can replace french accents
        
        ARGS
        your_col_inst (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
        
        RETURNS 
        pyspark col object

        NOTES
        To use function: `df = df.withColumn(col_name, clean_strs(your_col_inst))`
        
        """
        try:
            
            cln_strs = your_col_inst.cast("string")
            cln_strs = regexp_replace(cln_strs, r'\n{1,}', '') # remove any new lines
            cln_strs = regexp_replace(cln_strs, r'\'|\`|\“|\”', '') # remove apostropies
            cln_strs = regexp_replace(cln_strs, r'P\.O\.', 'PO') # remove PO periods
            cln_strs = regexp_replace(cln_strs, r'\.', ' ') # remove periods
            cln_strs = regexp_replace(cln_strs, r'-', ' ') # remove hyphens but leave a space (important for address extractor)
            cln_strs = regexp_replace(cln_strs, r'\(|\)', '') # remove parenthesis

            # BEAUTIFY
            cln_strs = upper(cln_strs)
            cln_strs = regexp_replace(cln_strs, r',{2,}', ',')
            cln_strs = regexp_replace(cln_strs, r"\s{1,}\,", ",")
            cln_strs = regexp_replace(cln_strs, r"\s{2,}", " ")
            
            if include_accents == True:

                    cln_strs = regexp_replace(cln_strs, r"É", "E") # accent aigu
                    cln_strs = regexp_replace(cln_strs, r"À", "A") # accent grave
                    cln_strs = regexp_replace(cln_strs, r"È", "E")
                    cln_strs = regexp_replace(cln_strs, r"Ù", "U")
                    cln_strs = regexp_replace(cln_strs, r"Â", "A") # Cironflexe
                    cln_strs = regexp_replace(cln_strs, r"Ê", "E")
                    cln_strs = regexp_replace(cln_strs, r"Î", "I")
                    cln_strs = regexp_replace(cln_strs, r"Ô", "O")
                    cln_strs = regexp_replace(cln_strs, r"Û", "U")
                    cln_strs = regexp_replace(cln_strs, r"Ë", "E") # Trema
                    cln_strs = regexp_replace(cln_strs, r"Ï", "I") 
                    cln_strs = regexp_replace(cln_strs, r"Ü", "U") 
                    cln_strs = regexp_replace(cln_strs, r"Ç", "C") 

            cln_strs = regexp_replace(cln_strs, r'[^\x00-\x7F]+', '') # remove any non-ascii characters LAST
            cln_strs = trim(cln_strs) # finalize standardization

            return cln_strs

        except AttributeError as e: 

            print("Missing required argument or incorrect data struct given for arg *your_col_instance*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
            raise

        except TypeError as e:

            print("Incorrect data given for arg *your_col_instance*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
            raise

        ###
