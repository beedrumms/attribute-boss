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
def clean_strs(your_strs):

        """
        Converts str to upper,  removes any double+ spaces, hyphens non-ASCII characters, apostrophes, periods, parenthesis, replaces french accents, strips trailing ws
        removes any double+ spaces
        removes apostrophes
        removes all non-ASCII characters 
        
        ARGS
        your_strs (pd.Series / list) = a pd.Series or list containing strs data you want to clean
        
        RETURNS
        pd.Series 

        NOTES
        commas are NOT removed
        
        """
   
        patterns = {r"": None,
                r"\n{1,}": "", # remove any new lines
                r"\'|\`|\“|\”": "",  # remove apostropies
                r"P\.O\.": "PO",  # remove PO periods
                r"\.": " ",  # remove periods
                r"-": " ",  # remove hyphens but leave a space (important for address extractor)
                r"\(|\)": "",  # remove parenthesis

                # BEAUTIFY
                r",{2,}": ",", 
                r"\s{1,}\,": ",",  

                r"É": "E",  # accent aigu
                r"À": "A",  # accent grave
                r"È": "E", 
                r"Ù": "U", 
                r"Â": "A",  # Cironflexe
                r"Ê": "E", 
                r"Î": "I", 
                r"Ô": "O", 
                r"Û": "U", 
                r"Ë": "E",  # Trema
                r"Ï": "I",  
                r"Ü": "U",  
                r"Ç": "C", 

                r"[^\x00-\x7F]+": ""}  # remove any non-ascii characters
        try:
                upper_strs = pd.Series(your_strs).str.upper() 
                cln_strs = upper_strs.replace(patterns, regex=True)
                cln_strs = cln_strs.replace(r"\s{2,}", " ", regex=True) # remove any spaces more than one 
                cln_strs = cln_strs.str.strip() # remove ws 

                return cln_strs

        except AttributeError as e: 

                print("Missing required argument or incorrect data struct given for arg *your_str*", "\n", "see func.__doc__ for more information on use", "\n")
                raise str(e)

        except TypeError as e:

                print("Incorrect data given for arg *your_str*", "\n", "see func.__doc__ for more information on use", "\n")
                raise str(e)
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

        NOTES
        this only works AFTER extract_() is used 

        it basically just gives you an indices list where the missing values are 
        
        """

        mask = np.array(attribute_list) == None 

        get_bool = np.array(range(len(mask)))

        indices_list = get_bool[mask]

        return indices_list
        ###