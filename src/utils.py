#%%
import pandas as pd
import numpy as np
import regex as re

def missing_val_handler(your_attribute):
        
        """
        replaces every missing value ("", None, or np.nan) or value labeled missing (NA, NONE, or NAN) to None value type 
        
        args 
        your_attribute (list, array, or pd.Series) your attribute list that you want to standardize missing vals
        
        returns 
        a pd.Series of your attribute with missing values standarized as None 
        
        """

        return pd.Series(your_attribute).replace({"NA":None, "Na":None, "nA":None, "na":None,
                                                  '':None, np.nan:None, 
                                                  "NONE":None, "none":None, "None":None, 
                                                  "NAN":None, "NaN":None, "nan":None})

#%%
def clean_str(your_str, include_accents=False):

        """
        Converts str to upper
        strips trailing ws
        removes any double+ spaces
        removes apostrophes
        removes all non-ASCII characters 
        
        Args
        your_str (str) = the string you want to quickly clean 
        
        returns 
        clean string
        
        """

        if your_str == None:
                cln_str = None
        
        else:
                # REMOVE ITEMS
                
                cln_str = re.sub(r'\n*', '', your_str) # remove any new lines
                cln_str = re.sub(r'[^\x00-\x7F]+', '', cln_str) # remove any non-ascii characters
                cln_str = re.sub(r'\'|\`|\“|\”|\,', '', cln_str) # remove apostropies

                # BEAUTIFY
                cln_str = str(cln_str).upper().strip()
                cln_str = re.sub(r"\s{2,}", " ", cln_str)

                if include_accents == True:

                        cln_str = re.sub(r"É", "E", cln_str) # accent aigu
                        cln_str = re.sub(r"À", "A", cln_str) # accent grave
                        cln_str = re.sub(r"È", "E", cln_str)
                        cln_str = re.sub(r"Ù", "U", cln_str)
                        cln_str = re.sub(r"Â", "A", cln_str) # Cironflexe
                        cln_str = re.sub(r"Ê", "E", cln_str)
                        cln_str = re.sub(r"Î", "I", cln_str)
                        cln_str = re.sub(r"Ô", "O", cln_str)
                        cln_str = re.sub(r"Û", "U", cln_str)
                        cln_str = re.sub(r"Ë", "E", cln_str) # Trema
                        cln_str = re.sub(r"Ï", "I", cln_str) 
                        cln_str = re.sub(r"Ü", "U", cln_str) 
                        cln_str = re.sub(r"Ç", "C", cln_str) 

        return cln_str
        ###

#%% 
def validate_attribute(attribute_list):

        """
        Checks whether an attribute exists and returns a list of indices of elements that didnt match the pattern 
        Must use the extract_attributes in advance 
        
        Args 
        attribute_list (list of str) = and list of strs 
        
        Returns 
        list of indices not matching attribute pattern
        
        """

        mask = np.array(attribute_list) == None 

        get_bool = np.array(range(len(mask)))

        indices_list = get_bool[mask]

        return indices_list
        ###