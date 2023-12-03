import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from .utils import safety_switch

import pyspark
from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, trim, upper
###

import regex as re

# %%
class SparkBoss:
        
        def __init__(self):
                self.name = self
                print("AttributeBoss is Ready")
                
        
        @safety_switch([pyspark.sql.column.Column])
        def tidy_str(self, attribute, 
                text_case='title', 
                stop_words=True, 
                periods=True, 
                hyphen=True, 
                comma=True, 
                symbols=True, 
                brackets=True,
                digits=True,
                apostrophies=True, 
                phone_nums=True,
                french_accents=True,
                remove=None,
                adjust=None):


                cln_strs = attribute.cast("string")
                cln_strs = upper(cln_strs)
        
                
                if apostrophies: 
                        cln_strs = regexp_replace(cln_strs, r'\`|\“|\”|\"', '') 
                        cln_strs = regexp_replace(cln_strs, r"'(?!S)", '') 

                if periods: 
                        cln_strs = regexp_replace(cln_strs, r'\.', ' ') 
                else:
                        cln_strs = regexp_replace(cln_strs, r'\.', '\. ') 

                if symbols: 
                        cln_strs = regexp_replace(cln_strs, r"[\@\,\=\?\*\&\#\:\;\/\+\\\~]", ' ') # remove useless punctuation and symbols

                if brackets:
                        cln_strs = regexp_replace(cln_strs, r"\([A-Z0-9\s-]{1,}\)", ' ') # remove everything in brackets 
                        cln_strs = regexp_replace(cln_strs, r"\(\)|\(|\)", " ") # left over brackets
                
                if phone_nums:
                        cln_strs = regexp_replace(cln_strs, r"[0-9]{1,}(\-|\.|\s|X|EXT|EXT\.)?[0-9]{1,}(\-|\.|\s|X|EXT|EXT\.)?", ' ') # remove all phone numbers

                if digits:
                        cln_strs = regexp_replace(cln_strs, r"\d{1,}", " ") # all digits

                # STOP WORDS 
                if stop_words:
                        cln_strs = regexp_replace(cln_strs, r"\bON\b|\bIN\b|\bFROM\b|\bFOR\b|\bTO\b|\bAT\b|\bBE\b|\bOF\b|\bTHE\b|\bAS\b|\bPER\b", " ")

                if hyphen:
                        cln_strs = regexp_replace(cln_strs, r'-', '') # remove hyphens AFTER doc term extraction

                else:
                        cln_strs = regexp_replace(cln_strs, r'-{2,}', '-') # remove multiple hyphens and replace with one
                        cln_strs = regexp_replace(cln_strs, r"\s{1,}\-\s{1,}|\s{1,}\-|\-\s{1,}", ' - ') # format hyphenated spaces

                if remove != None:
                        cln_strs = regexp_replace(cln_strs, remove, " ")
                
                if adjust != None:
                        cln_strs = regexp_replace(cln_strs, adjust[0], adjust[1])
                
                if french_accents: 
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
                        
                if text_case == 'lower':
                        cln_strs = lower(cln_strs)

                elif text_case == 'title':
                        cln_strs = initcap(cln_strs)
                
                elif text_case == 'upper':
                        pass

                else: 
                        print('No "upper", "lower", or "title" given (or incorrect input) -- defaulting to titlecase')
                

                # BEAUTIFY
                cln_strs = regexp_replace(cln_strs, r'\n{1,}', '') # remove any new lines
                cln_strs = regexp_replace(cln_strs, r"\s{2,}", " ") # remove wierd spacing
                cln_strs = regexp_replace(cln_strs, r'[^\x00-\x7F]+', '') # remove any non-ascii characters LAST
                cln_strs = trim(cln_strs) # finalize standardization
                
                return cln_strs
        
        @safety_switch([pyspark.sql.column.Column])
        def missing_val_handler(self, attribute):
        
                """
                replaces every missing value ("", None, or np.nan) or value labeled missing (NA, NONE, or NAN) to None value type 
                
                ARGS
                attribute (pyspark attribute) your attribute that you want to standardize the treatment of missing vals
                
                RETURNS
                a column object of your attribute with missing values standarized as None 

                NOTES
                attribute = df[col_name] 
                
                To use function: `df = df.withColumn(col_name, missing_val_handler(attribute))` 
                
                """

                missing_vals_standardized = when(attribute.isin(["NA", "Na", "nA", "na", "", "NONE", "none", "None", "NAN", "NaN", "nan"]), None).otherwise(attribute)

                return missing_vals_standardized

        @safety_switch([pyspark.sql.column.Column])
        def str_prep(self, attribute):
                
                """
                Converts str to upper, removes any double+ spaces, apostrophes, periods, hyphens, parentheses, non-ASCII characters, strips trailing ws, can replace french accents
                
                ARGS
                attribute (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
                
                RETURNS 
                pyspark col object

                NOTES
                To use function: `df = df.withColumn(col_name, clean_strs(attribute))`
                
                """

                cln_strs = attribute.cast("string")
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
        ###

        @safety_switch([pyspark.sql.column.Column])
        def standardize_address(self, attribute):
                
                """
                Reconciles and Converts Address Suffix, Prefix, and Street Directions to abbreviated version
                
                ARGS
                attribute (pyspark column instance) = column instance from a pyspark dataframe with strs containing addresses 
                
                RETURNS 
                pyspark col object 

                NOTES 
                To use function: `df = df.withColumn(col_name, standardize_address(attribute))`

                """
                
                address = self.str_prep(attribute)

                ## ENGLISH
                address = regexp_replace(address, r'\bNORTH\s?EAST\b', 'NE')
                address = regexp_replace(address, r'\bNORTH\s?WEST\b', 'NW')
                address = regexp_replace(address, r'\bSOUTH\s?EAST\b', 'SE')
                address = regexp_replace(address, r'\bSOUTH\s?WEST\b', 'SW')

                address = regexp_replace(address, r'\bNORTH\b', 'N') # north 
                address = regexp_replace(address, r'\bSOUTH\b', 'S') # south 
                address = regexp_replace(address, r'\bEAST\b', 'E') # east 
                address = regexp_replace(address, r'\bWEST\b', 'W') # west

                address = regexp_replace(address, r"\b(ALLY|ALLEE|ALLEY)\b", "ALY")
                address = regexp_replace(address, r"\b(ANNEX|ANNX|ANEX)\b", "ANX")
                address = regexp_replace(address, r"\bARCADE\b", "ARC")
                address = regexp_replace(address, r"\b(AVNUE|AV|AVENU|AVEN|AVN|AVENUE)\b", "AVE")
                address = regexp_replace(address, r"\b(BAYOU|BAYOO|BYU)\b", "BYU")
                address = regexp_replace(address, r"\bBEACH\b", "BCH")
                address = regexp_replace(address, r"\bBEND\b", "BND")
                address = regexp_replace(address, r"\b(BLUF|BLUFF)\b", "BLF")
                address = regexp_replace(address, r"\b(BLUFFS|BLFS)\b", "BLFS")
                address = regexp_replace(address, r"\b(BOT|BOTTOM|BOTTM)\b", "BTM")
                address = regexp_replace(address, r"\b(BOUL|BOULEVARD|BOULV)\b", "BLVD")
                address = regexp_replace(address, r"\b(BRANCH|BRNCH)\b", "BR")
                address = regexp_replace(address, r"\b(BRDGE|BRIDGE)\b", "BRG")
                address = regexp_replace(address, r"\bBROOK\b", "BRK")
                address = regexp_replace(address, r"\b(BROOKS|BRKS)\b", "BRKS")
                address = regexp_replace(address, r"\b(BURG|BG)\b", "BG")
                address = regexp_replace(address, r"\b(BGS|BURGS)\b", "BGS")
                address = regexp_replace(address, r"\b(BYPAS|BYPASS|BYPA|BYPS)\b", "BYP")
                address = regexp_replace(address, r"\b(CAMP|CMP)\b", "CP")
                address = regexp_replace(address, r"\b(CANYN|CNYN|CANYON|CYN)\b", "CYN")
                address = regexp_replace(address, r"\bCAPE\b", "CPE")
                address = regexp_replace(address, r"\b(CAUSEWAY|CAUSWA)\b", "CSWY")
                address = regexp_replace(address, r"\b(CENT|CEN|CNTR|CNTER|CENTER|CENTR|CENTRE)\b", "CTR")
                address = regexp_replace(address, r"\bCHEMIN\b", "CH")
                address = regexp_replace(address, r"\b(CTRS|CENTERS)\b", "CTRS")
                address = regexp_replace(address, r"\b(CRCLE|CRCL|CIRCL|CIRC|CIRCLE)\b", "CIR")
                address = regexp_replace(address, r"\b(CIRCLES|CIRS)\b", "CIRS")
                address = regexp_replace(address, r"\bCLIFF\b", "CLF")
                address = regexp_replace(address, r"\bCLIFFS\b", "CLFS")
                address = regexp_replace(address, r"\bCLUB\b", "CLB")
                address = regexp_replace(address, r"\b(CMN|COMMON)\b", "CMN")
                address = regexp_replace(address, r"\b(CMNS|COMMONS)\b", "CMNS")
                address = regexp_replace(address, r"\bCORNER\b", "COR")
                address = regexp_replace(address, r"\bCORNERS\b", "CORS")
                address = regexp_replace(address, r"\bCOURSE\b", "CRSE")
                address = regexp_replace(address, r"\bCOURT\b", "CT")
                address = regexp_replace(address, r"\bCOURTS\b", "CTS")
                address = regexp_replace(address, r"\bCOVE\b", "CV")
                address = regexp_replace(address, r"\b(COVES|CVS)\b", "CVS")
                address = regexp_replace(address, r"\bCREEK\b", "CRK")
                address = regexp_replace(address, r"\b(CRSNT|CRSENT|CRESCENT)\b", "CRES")
                address = regexp_replace(address, r"\b(CRST|CREST)\b", "CRST")
                address = regexp_replace(address, r"\b(CRSSNG|CROSSING)\b", "XING")
                address = regexp_replace(address, r"\b(XRD|CROSSROAD)\b", "XRD")
                address = regexp_replace(address, r"\b(CROSSROADS|XRDS)\b", "XRDS")
                address = regexp_replace(address, r"\b(CURV|CURVE)\b", "CURV")
                address = regexp_replace(address, r"\bDALE\b", "DL")
                address = regexp_replace(address, r"\bDAM\b", "DM")
                address = regexp_replace(address, r"\b(DVD|DIVIDE|DIV)\b", "DV")
                address = regexp_replace(address, r"\b(DRIVE|DRIV|DRV)\b", "DR")
                address = regexp_replace(address, r"\b(DRS|DRIVES)\b", "DRS")
                address = regexp_replace(address, r"\bESTATE\b", "EST")
                address = regexp_replace(address, r"\bESTATES\b", "ESTS")
                address = regexp_replace(address, r"\b(EXPR|EXPRESSWAY|EXPW|EXPRESS|EXP)\b", "EXPY")
                address = regexp_replace(address, r"\b(EXTENSION|EXTNSN|EXTN)\b", "EXT")
                address = regexp_replace(address, r"\bEXTS\b", "EXTS")
                address = regexp_replace(address, r"\bFALL\b", "FALL")
                address = regexp_replace(address, r"\bFALLS\b", "FLS")
                address = regexp_replace(address, r"\b(FERRY|FRRY)\b", "FRY")
                address = regexp_replace(address, r"\bFIELD\b", "FLD")
                address = regexp_replace(address, r"\bFIELDS\b", "FLDS")
                address = regexp_replace(address, r"\bFLAT\b", "FLT")
                address = regexp_replace(address, r"\bFLATS\b", "FLTS")
                address = regexp_replace(address, r"\bFORD\b", "FRD")
                address = regexp_replace(address, r"\b(FORDS|FRDS)\b", "FRDS")
                address = regexp_replace(address, r"\b(FOREST|FORESTS)\b", "FRST")
                address = regexp_replace(address, r"\b(FORG|FORGE)\b", "FRG")
                address = regexp_replace(address, r"\b(FORGES|FRGS)\b", "FRGS")
                address = regexp_replace(address, r"\bFORK\b", "FRK")
                address = regexp_replace(address, r"\bFORKS\b", "FRKS")
                address = regexp_replace(address, r"\b(FORT|FRT)\b", "FT")
                address = regexp_replace(address, r"\b(FRWY|FREEWAY|FREEWY|FRWAY)\b", "FWY")
                address = regexp_replace(address, r"\b(GRDN|GARDEN|GARDN|GDN|GRDEN)\b", "GDN")
                address = regexp_replace(address, r"\b(GARDENS|GRDNS)\b", "GDNS")
                address = regexp_replace(address, r"\b(GATEWAY|GTWAY|GATEWY|GATWAY)\b", "GTWY")
                address = regexp_replace(address, r"\bGLEN\b", "GLN")
                address = regexp_replace(address, r"\b(GLNS|GLENS)\b", "GLNS")
                address = regexp_replace(address, r"\bGREEN\b", "GRN")
                address = regexp_replace(address, r"\b(GREENS|GRNS)\b", "GRNS")
                address = regexp_replace(address, r"\b(GROV|GROVE)\b", "GRV")
                address = regexp_replace(address, r"\b(GRVS|GROVES)\b", "GRVS")
                address = regexp_replace(address, r"\b(HRBOR|HARBR|HARB|HARBOR)\b", "HBR")
                address = regexp_replace(address, r"\b(HBRS|HARBORS)\b", "HBRS")
                address = regexp_replace(address, r"\bHAVEN\b", "HVN")
                address = regexp_replace(address, r"\bHT\b", "HTS")
                address = regexp_replace(address, r"\b(HIWAY|HIGHWY|HWAY|HIGHWAY|HIWY)\b", "HWY")
                address = regexp_replace(address, r"\bHILL\b", "HL")
                address = regexp_replace(address, r"\bHILLS\b", "HLS")
                address = regexp_replace(address, r"\b(HLLW|HOLWS|HOLLOWS|HOLLOW)\b", "HOLW")
                address = regexp_replace(address, r"\bINLT\b", "INLT")
                address = regexp_replace(address, r"\b(ISLND|ISLAND)\b", "IS")
                address = regexp_replace(address, r"\b(ISLANDS|ISLNDS)\b", "ISS")
                address = regexp_replace(address, r"\bISLES\b", "ISLE")
                address = regexp_replace(address, r"\b(JUNCTN|JUNCTON|JCTN|JUNCTION|JCTION)\b", "JCT")
                address = regexp_replace(address, r"\b(JCTNS|JUNCTIONS)\b", "JCTS")
                address = regexp_replace(address, r"\bKEY\b", "KY")
                address = regexp_replace(address, r"\bKEYS\b", "KYS")
                address = regexp_replace(address, r"\b(KNOLL|KNOL)\b", "KNL")
                address = regexp_replace(address, r"\bKNOLLS\b", "KNLS")
                address = regexp_replace(address, r"\bLAKE\b", "LK")
                address = regexp_replace(address, r"\bLAKES\b", "LKS")
                address = regexp_replace(address, r"\bLAND\b", "LAND")
                address = regexp_replace(address, r"\b(LNDNG|LANDING)\b", "LNDG")
                address = regexp_replace(address, r"\bLANE\b", "LN")
                address = regexp_replace(address, r"\bLIGHT\b", "LGT")
                address = regexp_replace(address, r"\b(LIGHTS|LGTS)\b", "LGTS")
                address = regexp_replace(address, r"\bLOAF\b", "LF")
                address = regexp_replace(address, r"\bLOCK\b", "LCK")
                address = regexp_replace(address, r"\bLOCKS\b", "LCKS")
                address = regexp_replace(address, r"\b(LODGE|LDGE|LODG)\b", "LDG")
                address = regexp_replace(address, r"\bLOOPS\b", "LOOP")
                address = regexp_replace(address, r"\bMALL\b", "MALL")
                address = regexp_replace(address, r"\bMANOR\b", "MNR")
                address = regexp_replace(address, r"\bMANORS\b", "MNRS")
                address = regexp_replace(address, r"\b(MEADOW|MDW)\b", "MDW")
                address = regexp_replace(address, r"\b(MEDOWS|MDW|MEADOWS)\b", "MDWS")
                address = regexp_replace(address, r"\bMEWS\b", "MEWS")
                address = regexp_replace(address, r"\b(MILL|ML)\b", "ML")
                address = regexp_replace(address, r"\b(MLS|MILLS)\b", "MLS")
                address = regexp_replace(address, r"\b(MSN|MSSN|MISSN)\b", "MSN")
                address = regexp_replace(address, r"\b(MTWY|MOTORWAY)\b", "MTWY")
                address = regexp_replace(address, r"\b(MOUNT|MNT)\b", "MT")
                address = regexp_replace(address, r"\b(MNTAIN|MOUNTIN|MOUNTAIN|MNTN|MTIN)\b", "MTN")
                address = regexp_replace(address, r"\b(MNTNS|MOUNTAINS|MTNS)\b", "MTNS")
                address = regexp_replace(address, r"\bNECK\b", "NCK")
                address = regexp_replace(address, r"\b(ORCHARD|ORCHRD)\b", "ORCH")
                address = regexp_replace(address, r"\bOVL\b", "OVAL")
                address = regexp_replace(address, r"\b(OPAS|OVERPASS)\b", "OPAS")
                address = regexp_replace(address, r"\bPRK\b", "PARK")
                address = regexp_replace(address, r"\b(PARKS|PARK)\b", "PARK")
                address = regexp_replace(address, r"\b(PKWAY|PARKWAY|PARKWY|PKY)\b", "PKWY")
                address = regexp_replace(address, r"\b(PARKWAYS|PKWYS|PKWY)\b", "PKWY")
                address = regexp_replace(address, r"\bPASS\b", "PASS")
                address = regexp_replace(address, r"\b(PASSAGE|PSGE)\b", "PSGE")
                address = regexp_replace(address, r"\bPATHS\b", "PATH")
                address = regexp_replace(address, r"\bPIKES\b", "PIKE")
                address = regexp_replace(address, r"\b(PINE|PNE)\b", "PNE")
                address = regexp_replace(address, r"\bPINES\b", "PNES")
                address = regexp_replace(address, r"\bPL\b", "PL")
                address = regexp_replace(address, r"\bPLAIN\b", "PLN")
                address = regexp_replace(address, r"\bPLAINS\b", "PLNS")
                address = regexp_replace(address, r"\b(PLAZA|PLZA)\b", "PLZ")
                address = regexp_replace(address, r"\bPOINT\b", "PT")
                address = regexp_replace(address, r"\bPOINTS\b", "PTS")
                address = regexp_replace(address, r"\bPORT\b", "PRT")
                address = regexp_replace(address, r"\bPORTS\b", "PRTS")
                address = regexp_replace(address, r"\b(PRAIRIE|PRR)\b", "PR")
                address = regexp_replace(address, r"\bPROMENADE\b", "PROM")
                address = regexp_replace(address, r"\b(RAD|RADIEL|RADIAL)\b", "RADL")
                address = regexp_replace(address, r"\bRAMP\b", "RAMP")
                address = regexp_replace(address, r"\b(RNCHS|RANCHES|RANCH)\b", "RNCH")
                address = regexp_replace(address, r"\bRAPID\b", "RPD")
                address = regexp_replace(address, r"\bRAPIDS\b", "RPDS")
                address = regexp_replace(address, r"\bREST\b", "RST")
                address = regexp_replace(address, r"\b(RIDGE|RDGE)\b", "RDG")
                address = regexp_replace(address, r"\bRIDGES\b", "RDGS")
                address = regexp_replace(address, r"\b(RIVER|RVR|RIVR)\b", "RIV")
                address = regexp_replace(address, r"\bROAD\b", "RD")
                address = regexp_replace(address, r"\bROADS\b", "RDS")
                address = regexp_replace(address, r"\b(ROUTE|RTE)\b", "RTE")
                address = regexp_replace(address, r"\bROW\b", "ROW")
                address = regexp_replace(address, r"\bRUE\b", "RUE")
                address = regexp_replace(address, r"\bRUN\b", "RUN")
                address = regexp_replace(address, r"\bSHOAL\b", "SHL")
                address = regexp_replace(address, r"\bSHOALS\b", "SHLS")
                address = regexp_replace(address, r"\b(SHOAR|SHORE)\b", "SHR")
                address = regexp_replace(address, r"\b(SHOARS|SHORES)\b", "SHRS")
                address = regexp_replace(address, r"\b(SKWY|SKYWAY)\b", "SKWY")
                address = regexp_replace(address, r"\b(SPRING|SPNG|SPRNG)\b", "SPG")
                address = regexp_replace(address, r"\b(SPRINGS|SPNGS|SPRNGS)\b", "SPGS")
                address = regexp_replace(address, r"\bSPUR\b", "SPUR")
                address = regexp_replace(address, r"\b(SPUR|SPURS)\b", "SPUR")
                address = regexp_replace(address, r"\b(SQUARE|SQU|SQR|SQRE)\b", "SQ")
                address = regexp_replace(address, r"\b(SQS|SQRS|SQUARES)\b", "SQS")
                address = regexp_replace(address, r"\b(STATN|STN|STATION)\b", "STA")
                address = regexp_replace(address, r"\b(STRAV|STRVN|STRAVENUE|STRVNUE|STRAVN|STRAVEN)\b", "STRA")
                address = regexp_replace(address, r"\b(STREME|STREAM)\b", "STRM")
                address = regexp_replace(address, r"\b(STREET|STR|STRT)\b", "ST")
                address = regexp_replace(address, r"\b(STS|STREETS)\b", "STS")
                address = regexp_replace(address, r"\b(SUMIT|SUMMIT|SUMITT)\b", "SMT")
                address = regexp_replace(address, r"\b(TERRACE|TERR)\b", "TER")
                address = regexp_replace(address, r"\b(THROUGHWAY|TRWY)\b", "TRWY")
                address = regexp_replace(address, r"\b(TRACE|TRACES)\b", "TRCE")
                address = regexp_replace(address, r"\b(TRACK|TRK|TRKS|TRACKS)\b", "TRAK")
                address = regexp_replace(address, r"\b(TRFY|TRAFFICWAY)\b", "TRFY")
                address = regexp_replace(address, r"\b(TRAIL|TRLS|TRAILS)\b", "TRL")
                address = regexp_replace(address, r"\b(TRLRS|TRAILER)\b", "TRLR")
                address = regexp_replace(address, r"\b(TUNNL|TUNLS|TUNNEL|TUNNELS|TUNEL)\b", "TUNL")
                address = regexp_replace(address, r"\b(TPKE|TURNPK|TRNPK|TURNPIKE)\b", "TPKE")
                address = regexp_replace(address, r"\b(UPAS|UNDERPASS)\b", "UPAS")
                address = regexp_replace(address, r"\bUNION\b", "UN")
                address = regexp_replace(address, r"\b(UNIONS|UNS)\b", "UNS")
                address = regexp_replace(address, r"\b(VLLY|VALLY|VALLEY)\b", "VLY")
                address = regexp_replace(address, r"\bVALLEYS\b", "VLYS")
                address = regexp_replace(address, r"\b(VIADCT|VDCT|VIADUCT)\b", "VIA")
                address = regexp_replace(address, r"\bVIEW\b", "VW")
                address = regexp_replace(address, r"\bVIEWS\b", "VWS")
                address = regexp_replace(address, r"\b(VILL|VILLAG|VILLG|VILLAGE|VILLIAGE)\b", "VLG")
                address = regexp_replace(address, r"\bVILLAGES\b", "VLGS")
                address = regexp_replace(address, r"\bVILLE\b", "VL")
                address = regexp_replace(address, r"\b(VSTA|VST|VISTA|VIST)\b", "VIS")
                address = regexp_replace(address, r"\bWALK\b", "WALK")
                address = regexp_replace(address, r"\b(WALK|WALKS)\b", "WALK")
                address = regexp_replace(address, r"\bWALL\b", "WALL")
                address = regexp_replace(address, r"\bWY\b", "WAY")
                address = regexp_replace(address, r"\bWAYS\b", "WAYS")
                address = regexp_replace(address, r"\b(WL|WELL)\b", "WL")
                address = regexp_replace(address, r"\bWELLS\b", "WLS")
        
                return address

        @safety_switch([pyspark.sql.column.Column])
        def standardize_postal_code(self, attribute):
                
                """
                Cleans out common errors in postal code representations 

                ARGS 
                attribute (pyspark column instance) = column instance from a pyspark dataframe with strs containing postal code 

                RETURNS 
                pyspark col object 

                NOTES 
                To use function: `df = df.withColumn(col_name, standardize_postal_code(attribute))`

                """

                code = attribute.cast("string")
                code = regexp_replace(code, r"\.|-|_", "")
                code = regexp_replace(code, r"\s{1,}|\n{1,}", "")
                code = trim(upper(code))
                        
                return code

        @safety_switch([pyspark.sql.column.Column])
        def standardize_phone_number(self, attribute):
                
                """
                Standardize phone numbers
                
                ARGS
                attribute (pyspark column instance) = column instance from a pyspark dataframe with str containing phone nums
                
                RETURNS 
                pyspark col object 

                NOTES 
                To use function: `df = df.withColumn(col_name, standardize_phone_number(attribute))`
                
                """
                num = attribute.cast("string")
                num = regexp_replace(num, r"\.|-|_", "")
                num = regexp_replace(num, r"\(|\)", "")
                num = regexp_replace(num, r"\s{1,}|\n{1,}", "")
                num = trim(num)

                return num


        @safety_switch([pyspark.sql.column.Column])
        def standardize_province_state(self, attribute):
                """
                Reconcile and convert province, state, or territory titles to abbreviated version
                
                ARGS
                attribute (pyspark column instance) = column instance from a pyspark dataframe 
                
                RETURNS 
                pyspark col object 

                NOTES 
                To use function: `df = df.withColumn(col_name, standardize_province_state(attribute))`
                
                """
                prov_state = self.str_prep(attribute)

                prov_state = regexp_replace(prov_state, r"\bALABAMA\b", "AL")
                prov_state = regexp_replace(prov_state, r"\bALASKA\b", "AK")
                prov_state = regexp_replace(prov_state, r"\bAMERICAN SAMOA\b", "AS")
                prov_state = regexp_replace(prov_state, r"\bARIZONA\b", "AZ")
                prov_state = regexp_replace(prov_state, r"\bARKANSAS\b", "AR")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES AFRICA\b", "AE")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES AMERICA\b", "AA")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES CANADA\b", "AE")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES EUROPE\b", "AE")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES MIDDLE EAST\b", "AE")
                prov_state = regexp_replace(prov_state, r"\bARMED FORCES PACIFIC\b", "AP")
                prov_state = regexp_replace(prov_state, r"\bCALIFORNIA\b", "CA")
                prov_state = regexp_replace(prov_state, r"\bCOLORADO\b", "CO")
                prov_state = regexp_replace(prov_state, r"\bCONNETICUT\b", "CT")
                prov_state = regexp_replace(prov_state, r"\bDELAWARE\b", "DE")
                prov_state = regexp_replace(prov_state, r"\bDISTRICT OF COLUMBIA\b", "DC")
                prov_state = regexp_replace(prov_state, r"\bFLORIDA\b", "FL")
                prov_state = regexp_replace(prov_state, r"\bGEORGIA\b", "GA")
                prov_state = regexp_replace(prov_state, r"\bGUAM\b", "GU")
                prov_state = regexp_replace(prov_state, r"\bHAWAII\b", "HI")
                prov_state = regexp_replace(prov_state, r"\bIDAHO\b", "ID")
                prov_state = regexp_replace(prov_state, r"\bILLINOIS\b", "IL")
                prov_state = regexp_replace(prov_state, r"\bINDIANA\b", "IN")
                prov_state = regexp_replace(prov_state, r"\bIOWA\b", "IA")
                prov_state = regexp_replace(prov_state, r"\bKANSAS\b", "KS")
                prov_state = regexp_replace(prov_state, r"\bKENTUCKY\b", "KY")
                prov_state = regexp_replace(prov_state, r"\bLOUISIANA\b", "LA")
                prov_state = regexp_replace(prov_state, r"\bMAINE\b", "ME")
                prov_state = regexp_replace(prov_state, r"\bMARSHALL ISLANDS\b", "MH")
                prov_state = regexp_replace(prov_state, r"\bMARYLAND\b", "MD")
                prov_state = regexp_replace(prov_state, r"\bMASSACHUSETTS\b", "MA")
                prov_state = regexp_replace(prov_state, r"\bMICHIGAN\b", "MI")
                prov_state = regexp_replace(prov_state, r"\bMICRONESIA\b", "FM")
                prov_state = regexp_replace(prov_state, r"\bMINNESOTA\b", "MN")
                prov_state = regexp_replace(prov_state, r"\bMINOR OUTLYING ISLANDS\b", "UM")
                prov_state = regexp_replace(prov_state, r"\bMISSISSIPPI\b", "MS")
                prov_state = regexp_replace(prov_state, r"\bMONTANA\b", "MT")
                prov_state = regexp_replace(prov_state, r"\bNEBRASKA\b", "NE")
                prov_state = regexp_replace(prov_state, r"\bNEVADA\b", "NV")
                prov_state = regexp_replace(prov_state, r"\bNEW HAMPSHIRE\b", "NH")
                prov_state = regexp_replace(prov_state, r"\bNEW JERSEY\b", "NJ")
                prov_state = regexp_replace(prov_state, r"\bNEW MEXICO\b", "NM")
                prov_state = regexp_replace(prov_state, r"\bNEW YORK\b", "NY")
                prov_state = regexp_replace(prov_state, r"\bNORTH CAROLINA\b", "NC")
                prov_state = regexp_replace(prov_state, r"\bNORTH DAKOTA\b", "ND")
                prov_state = regexp_replace(prov_state, r"\bNORTHERN MARIANA ISLANDS\b", "MP")
                prov_state = regexp_replace(prov_state, r"\bOHIO\b", "OH")
                prov_state = regexp_replace(prov_state, r"\bOKLAHOMA\b", "OK")
                prov_state = regexp_replace(prov_state, r"\bOREGON\b", "OR")
                prov_state = regexp_replace(prov_state, r"\bPALAU\b", "PW")
                prov_state = regexp_replace(prov_state, r"\bPENNSLYVANIA\b", "PA")
                prov_state = regexp_replace(prov_state, r"\bPUERTO RICO\b", "PR")
                prov_state = regexp_replace(prov_state, r"\bRHODE ISLAND\b", "RI")
                prov_state = regexp_replace(prov_state, r"\bSOUTH CAROLINA\b", "SC")
                prov_state = regexp_replace(prov_state, r"\bSOUTH DAKOTA\b", "SD")
                prov_state = regexp_replace(prov_state, r"\bTENNESSEE\b", "TN")
                prov_state = regexp_replace(prov_state, r"\bTEXAS\b", "TX")
                prov_state = regexp_replace(prov_state, r"\bUTAH\b", "UT")
                prov_state = regexp_replace(prov_state, r"\bVERMONT\b", "VT")
                prov_state = regexp_replace(prov_state, r"\bVIRGINIA\b", "VA")
                prov_state = regexp_replace(prov_state, r"\bVIRGIN ISLANDS\b", "VI")
                prov_state = regexp_replace(prov_state, r"\bWASHINGTON\b", "WA")
                prov_state = regexp_replace(prov_state, r"\bWEST VIRGINIA\b", "WV")
                prov_state = regexp_replace(prov_state, r"\bWISCONSIN\b", "WI")
                prov_state = regexp_replace(prov_state, r"\bWYOMING\b", "WY")
                prov_state = regexp_replace(prov_state, r"\bALBERTA\b", "AB")
                prov_state = regexp_replace(prov_state, r"\bBRITISH COLUMBIA\b", "BC")
                prov_state = regexp_replace(prov_state, r"\bMANITOBA\b", "MB")
                prov_state = regexp_replace(prov_state, r"\bNEW BRUNSWICK\b", "NB")
                prov_state = regexp_replace(prov_state, r"\bNEWFOUNDLAND AND LABRADOR\b", "NL")
                prov_state = regexp_replace(prov_state, r"\bNORTHWEST TERRITORIES\b", "NT")
                prov_state = regexp_replace(prov_state, r"\bNOVA SCOTIA\b", "NS")
                prov_state = regexp_replace(prov_state, r"\bNUNAVUT\b", "NU")
                prov_state = regexp_replace(prov_state, r"\bONTARIO\b|\bONT\b", "ON")
                prov_state = regexp_replace(prov_state, r"\bPRINCE EDWARD ISLAND\b", "PE")
                prov_state = regexp_replace(prov_state, r"\bQUEBEC\b", "QC")
                prov_state = regexp_replace(prov_state, r"\bSASKATCHEWAN\b", "SK")
                prov_state = regexp_replace(prov_state, r"\bYUKON\b", "YT")

                return prov_state
                
        @safety_switch([pyspark.sql.column.Column])
        def extract_address(self, attribute):

                """
                Looks for the basic street address in a string
                street number + street name+ + street direction? 
                
                ARGS
                attribute (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
                
                RETURNS
                pyspark col object

                NOTES
                make sure to pass this through the standardizer -- every prefix and suffix should be uppercase and abbreviated

                to use function: `df.withColumn('col_name', extract_address(address_attribute)) 
                        
                
                """
                ixes = re.compile(r"(\bN\b|\bS\b|\bE\b|\bW\b|\bNE\b|\bNW\b|\bSE\b|\bSW\b|\bALY\b|\bANX\b|\bARC\b|\bAVE\b|\bBYU\b|\bBCH\b|\bBND\b|\bBLF\b|\bBLFS\b|\bBTM\b|\bBLVD\b|\bBR\b|\bBRG\b|\bBRK\b|\bBRKS\b|\bBG\b|\bBGS\b|\bBYP\b|\bCP\b|\bCYN\b|\bCPE\b|\bCSWY\b|\bCOURS\b|\bCH\b|\bCTR\b|\bCTRS\b|\bCIR\b|\bCIRS\b|\bCLF\b|\bCLFS\b|\bCLB\b|\bCMN\b|\bCMNS\b|\bCOR\b|\bCORS\b|\bCRSE\b|\bCT\b|\bCTS\b|\bCV\b|\bCVS\b|\bCRK\b|\bCRES\b|\bCRST\b|\bXING\b|\bXRD\b|\bXRDS\b|\bCURV\b|\bDL\b|\bDM\b|\bDV\b|\bDR\b|\bDRS\b|\bEST\b|\bESTS\b|\bEXPY\b|\bEXT\b|\bEXTS\b|\bFALL\b|\bFLS\b|\bFRY\b|\bFLD\b|\bFLDS\b|\bFLT\b|\bFLTS\b|\bFRD\b|\bFRDS\b|\bFRST\b|\bFRG\b|\bFRGS\b|\bFRK\b|\bFRKS\b|\bFT\b|\bFWY\b|\bGDN\b|\bGDNS\b|\bGTWY\b|\bGLN\b|\bGLNS\b|\bGRN\b|\bGRNS\b|\bGRV\b|\bGRVS\b|\bHBR\b|\bHBRS\b|\bHVN\b|\bHTS\b|\bHWY\b|\bHL\b|\bHLS\b|\bHOLW\b|\bINLT\b|\bIS\b|\bISS\b|\bISLE\b|\bJCT\b|\bJCTS\b|\bKY\b|\bKYS\b|\bKNL\b|\bKNLS\b|\bLK\b|\bLKS\b|\bLAND\b|\bLNDG\b|\bLN\b|\bLGT\b|\bLGTS\b|\bLF\b|\bLCK\b|\bLCKS\b|\bLDG\b|\bLOOP\b|\bMALL\b|\bMNR\b|\bMNRS\b|\bMDW\b|\bMDWS\b|\bMEWS\b|\bML\b|\bMLS\b|\bMSN\b|\bMTWY\b|\bMT\b|\bMTN\b|\bMTNS\b|\bNCK\b|\bORCH\b|\bOVAL\b|\bOPAS\b|\bPARK\b|\bPARK\b|\bPKWY\b|\bPKWY\b|\bPASS\b|\bPSGE\b|\bPATH\b|\bPIKE\b|\bPNE\b|\bPNES\b|\bPL\b|\bPLN\b|\bPLNS\b|\bPLZ\b|\bPT\b|\bPTS\b|\bPROM\b|\bPRT\b|\bPRTS\b|\bPR\b|\bRADL\b|\bRAMP\b|\bRNCH\b|\bRPD\b|\bRPDS\b|\bRST\b|\bRDG\b|\bRDGS\b|\bRIV\b|\bRD\b|\bRDS\b|\bRTE\b|\bROW\b|\bRUE\b|\bRUN\b|\bSHL\b|\bSHLS\b|\bSHR\b|\bSHRS\b|\bSKWY\b|\bSPG\b|\bSPGS\b|\bSPUR\b|\bSPUR\b|\bSQ\b|\bSQS\b|\bSTA\b|\bSTRA\b|\bSTRM\b|\bST\b|\bSTS\b|\bSMT\b|\bTER\b|\bTRWY\b|\bTRCE\b|\bTRAK\b|\bTRFY\b|\bTRL\b|\bTRLR\b|\bTUNL\b|\bTPKE\b|\bUPAS\b|\bUN\b|\bUNS\b|\bVLY\b|\bVLYS\b|\bVIA\b|\bVW\b|\bVWS\b|\bVLG\b|\bVLGS\b|\bVL\b|\bVIS\b|\bWALK\b|\bWALK\b|\bWALL\b|\bWAY\b|\bWAYS\b|\bWL\b|\bWLS\b)")

                matches = regexp_extract(attribute, fr"\d+\s\w+\s\w?\s?{ixes.pattern}\s?{ixes.pattern}?\s?{ixes.pattern}?\s?{ixes.pattern}?", 0)
                extracted = when(matches.isin([""]), None).otherwise(matches)
                extracted = trim(extracted)
                
                return extracted
        
        @safety_switch([pyspark.sql.column.Column])
        def extract_postal_code(self, attribute):

                """
                Looks for Canadian and US postal codes / zips
                
                ARGS
                attribute (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
                
                RETURNS
                cleaned col instance  

                NOTES
                make sure to pass this through the standardizer -- everything should be uppercase

                to use function: `df.withColumn('col_name', extract_postal_code(postal_attribute)) 
                
                """
                
                code = re.compile(r"\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b|\b\d{5}\b")
                matches = regexp_extract(attribute, fr"{code.pattern}", 0)
                extracted = when(matches.isin([""]), None).otherwise(matches)

                return extracted

        @safety_switch([pyspark.sql.column.Column])
        def extract_phone_number(self, attribute):

                """
                Looks for Canadian and US postal codes / zips
                
                ARGS
                attribute (pyspark col instance) = a col instance that you want to quickly clean -- must be str type 
                
                RETURNS
                pyspark col object

                NOTES
                make sure to pass this through the standardizer -- everything should be uppercase

                this function gets rid of a country code and only takes the last 10 digits

                to use function: `df.withColumn('col_name', extract_phone_number(attribute)) 
                
                """
                
                num = re.compile(r"\d\d\d\s?\d\d\d\s?\d\d\d\d\b")
                matches = regexp_extract(attribute, fr"{num.pattern}", 0)
                extracted = when(matches.isin([""]), None).otherwise(matches)

                return extracted
