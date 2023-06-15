### PURPOSE
## Identify street address 

#%% IMPORT PACKAGES
# basic py packages
import pandas as pd
import numpy as np
import regex as re

import pyspark
from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, trim, upper
###

# %%
class Extract:

    def __init__(self):

        print("Initalized - push pyspark col instances as argument")

    def extract_address(self, address_col_inst):

        """
        Looks for the basic street address in a string
        street number + street name+ + street direction? 
        
        ARGS
        address_col_inst (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
        
        RETURNS
        pyspark col object

        NOTES
        make sure to pass this through the standardizer -- every prefix and suffix should be uppercase and abbreviated

        to use function: `df.withColumn('col_name', extract_address(address_col_inst)) 
                
        
        """
        try:
            
            ixes = re.compile(r"(\bN\b|\bS\b|\bE\b|\bW\b|\bNE\b|\bNW\b|\bSE\b|\bSW\b|\bALY\b|\bANX\b|\bARC\b|\bAVE\b|\bBYU\b|\bBCH\b|\bBND\b|\bBLF\b|\bBLFS\b|\bBTM\b|\bBLVD\b|\bBR\b|\bBRG\b|\bBRK\b|\bBRKS\b|\bBG\b|\bBGS\b|\bBYP\b|\bCP\b|\bCYN\b|\bCPE\b|\bCSWY\b|\bCOURS\b|\bCH\b|\bCTR\b|\bCTRS\b|\bCIR\b|\bCIRS\b|\bCLF\b|\bCLFS\b|\bCLB\b|\bCMN\b|\bCMNS\b|\bCOR\b|\bCORS\b|\bCRSE\b|\bCT\b|\bCTS\b|\bCV\b|\bCVS\b|\bCRK\b|\bCRES\b|\bCRST\b|\bXING\b|\bXRD\b|\bXRDS\b|\bCURV\b|\bDL\b|\bDM\b|\bDV\b|\bDR\b|\bDRS\b|\bEST\b|\bESTS\b|\bEXPY\b|\bEXT\b|\bEXTS\b|\bFALL\b|\bFLS\b|\bFRY\b|\bFLD\b|\bFLDS\b|\bFLT\b|\bFLTS\b|\bFRD\b|\bFRDS\b|\bFRST\b|\bFRG\b|\bFRGS\b|\bFRK\b|\bFRKS\b|\bFT\b|\bFWY\b|\bGDN\b|\bGDNS\b|\bGTWY\b|\bGLN\b|\bGLNS\b|\bGRN\b|\bGRNS\b|\bGRV\b|\bGRVS\b|\bHBR\b|\bHBRS\b|\bHVN\b|\bHTS\b|\bHWY\b|\bHL\b|\bHLS\b|\bHOLW\b|\bINLT\b|\bIS\b|\bISS\b|\bISLE\b|\bJCT\b|\bJCTS\b|\bKY\b|\bKYS\b|\bKNL\b|\bKNLS\b|\bLK\b|\bLKS\b|\bLAND\b|\bLNDG\b|\bLN\b|\bLGT\b|\bLGTS\b|\bLF\b|\bLCK\b|\bLCKS\b|\bLDG\b|\bLOOP\b|\bMALL\b|\bMNR\b|\bMNRS\b|\bMDW\b|\bMDWS\b|\bMEWS\b|\bML\b|\bMLS\b|\bMSN\b|\bMTWY\b|\bMT\b|\bMTN\b|\bMTNS\b|\bNCK\b|\bORCH\b|\bOVAL\b|\bOPAS\b|\bPARK\b|\bPARK\b|\bPKWY\b|\bPKWY\b|\bPASS\b|\bPSGE\b|\bPATH\b|\bPIKE\b|\bPNE\b|\bPNES\b|\bPL\b|\bPLN\b|\bPLNS\b|\bPLZ\b|\bPT\b|\bPTS\b|\bPROM\b|\bPRT\b|\bPRTS\b|\bPR\b|\bRADL\b|\bRAMP\b|\bRNCH\b|\bRPD\b|\bRPDS\b|\bRST\b|\bRDG\b|\bRDGS\b|\bRIV\b|\bRD\b|\bRDS\b|\bRTE\b|\bROW\b|\bRUE\b|\bRUN\b|\bSHL\b|\bSHLS\b|\bSHR\b|\bSHRS\b|\bSKWY\b|\bSPG\b|\bSPGS\b|\bSPUR\b|\bSPUR\b|\bSQ\b|\bSQS\b|\bSTA\b|\bSTRA\b|\bSTRM\b|\bST\b|\bSTS\b|\bSMT\b|\bTER\b|\bTRWY\b|\bTRCE\b|\bTRAK\b|\bTRFY\b|\bTRL\b|\bTRLR\b|\bTUNL\b|\bTPKE\b|\bUPAS\b|\bUN\b|\bUNS\b|\bVLY\b|\bVLYS\b|\bVIA\b|\bVW\b|\bVWS\b|\bVLG\b|\bVLGS\b|\bVL\b|\bVIS\b|\bWALK\b|\bWALK\b|\bWALL\b|\bWAY\b|\bWAYS\b|\bWL\b|\bWLS\b)")

            matches = regexp_extract(address_col_inst, fr"\d+\s\w+\s\w?\s?{ixes.pattern}\s?{ixes.pattern}?\s?{ixes.pattern}?\s?{ixes.pattern}?", 0)
            extracted = when(matches.isin([""]), None).otherwise(matches)
            extracted = trim(extracted)
            
            return extracted

        except AttributeError as e: 

                print("Missing required argument or incorrect data struct given for arg *address_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise

        except TypeError as e:

                print("Incorrect data given for arg *address_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise

    # %%

    def extract_postal_code(self, postal_col_inst):

        """
        Looks for Canadian and US postal codes / zips
        
        ARGS
        postal_col_inst (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
        
        RETURNS
        cleaned col instance  

        NOTES
        make sure to pass this through the standardizer -- everything should be uppercase

        to use function: `df.withColumn('col_name', extract_postal_code(postal_col_inst)) 
        
        """
        try:

            code = re.compile(r"\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b|\b\d{5}\b")

            matches = regexp_extract(postal_col_inst, fr"{code.pattern}", 0)
            extracted = when(matches.isin([""]), None).otherwise(matches)

            return extracted

        except AttributeError as e: 

                print("Missing required argument or incorrect data struct given for arg *postal_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise

        except TypeError as e:

                print("Incorrect data given for arg *postal_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise
###
    # %%

    def extract_phone_number(self, phone_col_inst):

        """
        Looks for Canadian and US postal codes / zips
        
        ARGS
        postal_col_inst (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
        
        RETURNS
        pyspark col object

        NOTES
        make sure to pass this through the standardizer -- everything should be uppercase

        this function gets rid of a country code and only takes the last 10 digits

        to use function: `df.withColumn('col_name', extract_phone_number(phone_col_inst)) 
        
        """
        try:

            num = re.compile(r"\d\d\d\s?\d\d\d\s?\d\d\d\d\b")

            matches = regexp_extract(phone_col_inst, fr"{num.pattern}", 0)
            extracted = when(matches.isin([""]), None).otherwise(matches)

            return extracted

        except AttributeError as e: 

                print("Missing required argument or incorrect data struct given for arg *phone_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise

        except TypeError as e:

                print("Incorrect data given for arg *phone_col_inst*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                raise
