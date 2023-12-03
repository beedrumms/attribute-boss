
#%%
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from .utils import safety_switch 

import pandas as pd
import numpy as np
import regex as re
###

#%%
class PyBoss:
        
        def __init__(self):
                self.name = self
                
                print("AttributeBoss is Ready")

        @safety_switch([list, pd.Series])
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


                # cln_strs = attribute.cast("string")
                cln_strs = [i.upper() for i in attribute]
        
                
                if apostrophies: 
                        cln_strs = re.sub(r'\`|\“|\”|\"', '', cln_strs) 
                        cln_strs = re.sub(r"'(?!S)", '', cln_strs) 

                if periods: 
                        cln_strs = re.sub(r'\.', ' ', cln_strs) 
                else:
                        cln_strs = re.sub(r'\.', '\. ', cln_strs) 

                if symbols: 
                        cln_strs = re.sub(r"[\@\,\=\?\*\&\#\:\;\/\+\\\~\|]", ' ', cln_strs) # remove useless punctuation and symbols

                if brackets:
                        cln_strs = re.sub(r"\([A-Z0-9\s-]{1,}\)", ' ', cln_strs) # remove everything in brackets 
                        cln_strs = re.sub(r"\(\)|\(|\)", " ", cln_strs) # left over brackets
                
                if phone_nums:
                        cln_strs = re.sub(r"[0-9]{1,}(\-|\.|\s|X|EXT|EXT\.)?[0-9]{1,}(\-|\.|\s|X|EXT|EXT\.)?", ' ', cln_strs) # remove all phone numbers

                if digits:
                        cln_strs = re.sub(r"\d{1,}", " ", cln_strs) # all digits

                # STOP WORDS 
                if stop_words:
                        cln_strs = re.sub(r"\bON\b|\bIN\b|\bFROM\b|\bFOR\b|\bTO\b|\bAT\b|\bBE\b|\bOF\b|\bTHE\b|\bAS\b|\bPER\b", " ", cln_strs)

                if hyphen:
                        cln_strs = re.sub(r'-', '', cln_strs) # remove hyphens AFTER doc term extraction

                else:
                        cln_strs = re.sub(r'-{2,}', '-', cln_strs) # remove multiple hyphens and replace with one
                        cln_strs = re.sub(r"\s{1,}\-\s{1,}|\s{1,}\-|\-\s{1,}", ' - ', cln_strs) # format hyphenated spaces

                if remove != None:
                        cln_strs = re.sub(remove, " ", cln_strs)
                
                if adjust != None:
                        cln_strs = re.sub(adjust[0], adjust[1], cln_strs)
                
                if french_accents: 
                        cln_strs = re.sub( r"É", "E") # accent aigu
                        cln_strs = re.sub( r"À", "A") # accent grave
                        cln_strs = re.sub( r"È", "E")
                        cln_strs = re.sub( r"Ù", "U")
                        cln_strs = re.sub( r"Â", "A") # Cironflexe
                        cln_strs = re.sub( r"Ê", "E")
                        cln_strs = re.sub( r"Î", "I")
                        cln_strs = re.sub( r"Ô", "O")
                        cln_strs = re.sub( r"Û", "U")
                        cln_strs = re.sub( r"Ë", "E") # Trema
                        cln_strs = re.sub( r"Ï", "I") 
                        cln_strs = re.sub( r"Ü", "U") 
                        cln_strs = re.sub( r"Ç", "C") 
                
                if text_case == 'lower':
                        cln_strs = lower(cln_strs)

                elif text_case == 'title':
                        cln_strs = initcap(cln_strs)
                
                elif text_case == 'upper':
                        pass

                else: 
                        print('No "upper", "lower", or "title" given (or incorrect input) -- defaulting to titlecase')
                

                # BEAUTIFY
                cln_strs = re.sub( r'\n{1,}', '') # remove any new lines
                cln_strs = re.sub( r"\s{2,}", " ") # remove wierd spacing
                cln_strs = re.sub( r'[^\x00-\x7F]+', '') # remove any non-ascii characters LAST
                cln_strs = trim(cln_strs) # finalize standardization
                
                return cln_strs

        @safety_switch([list, pd.Series])
        def missing_val_handler(self, attribute):
                
                """
                replaces every missing value ("", None, or np.nan) or value labeled missing (NA, NONE, or NAN) to None value type 
                
                args 
                attribute (list, array, or pd.Series) your attribute list that you want to standardize missing vals
                
                returns 
                a pd.Series of your attribute with missing values standarized as None 
                
                """
                
                return pd.Series(attribute).replace({"NA":None, "Na":None, "nA":None, "na":None,
                                                  '':None, np.nan:None, 
                                                  "NONE":None, "none":None, "None":None, 
                                                  "NAN":None, "NaN":None, "nan":None})
        
        @safety_switch([list, pd.Series])
        def str_prep(self, attribute):
                
                """
                Converts str to upper,  removes any double+ spaces, hyphens non-ASCII characters, apostrophes, periods, parenthesis, replaces french accents, strips trailing ws
                removes any double+ spaces
                removes apostrophes
                removes all non-ASCII characters 
                
                ARGS
                attribute (pd.Series / list) = a pd.Series or list containing strs data you want to clean
                
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
                
                upper_strs = pd.Series(attribute).str.upper() 
                cln_strs = upper_strs.replace(patterns, regex=True)
                cln_strs = cln_strs.replace(r"\s{2,}", " ", regex=True) # remove any spaces more than one 
                cln_strs = cln_strs.str.strip() # remove ws 

                return cln_strs

        @safety_switch([list, pd.Series])
        def standardize_address(self, attribute):

                """
                Converts str to upper,  removes any double+ spaces, hyphens non-ASCII characters, apostrophes, periods, parenthesis, replaces french accents, strips trailing ws
                
                ARGS
                attribute (pd.Series) = a pd.Series or list containing address data you want to standardize
                
                RETURNS
                pd.Series 

                NOTES
                commas are NOT removed
                
                """

                ixes =  {r"\bNORTH\s?EAST\b": "NE",
                        r"\bNORTH\s?WEST\b": "NW",
                        r"\bSOUTH\s?EAST\b": "SE",
                        r"\bSOUTH\s?WEST\b": "SW",

                        r"\bNORTH\b": "N", # north 
                        r"\bSOUTH\b": "S", # south 
                        r"\bEAST\b": "E", # east 
                        r"\bWEST\b": "W", # west

                        r"\b(ALLY|ALLEE|ALLEY|ALLÉE)\b": "ALY",
                        r"\b(ANNEX|ANNX|ANEX)\b": "ANX",
                        r"\bARCADE\b": "ARC",
                        r"\b(AVNUE|AV|AVENU|AVEN|AVN|AVENUE)\b": "AVE",
                        r"\b(BAYOU|BAYOO|BYU)\b": "BYU",
                        r"\bBEACH\b": "BCH",
                        r"\bBEND\b": "BND",
                        r"\b(BLUF|BLUFF)\b": "BLF",
                        r"\b(BLUFFS|BLFS)\b": "BLFS",
                        r"\b(BOT|BOTTOM|BOTTM)\b": "BTM",
                        r"\b(BOUL|BOULEVARD|BOULV)\b": "BLVD",
                        r"\b(BRANCH|BRNCH)\b": "BR",
                        r"\b(BRDGE|BRIDGE)\b": "BRG",
                        r"\bBROOK\b": "BRK",
                        r"\b(BROOKS|BRKS)\b": "BRKS",
                        r"\b(BURG|BG)\b": "BG",
                        r"\b(BGS|BURGS)\b": "BGS",
                        r"\b(BYPAS|BYPASS|BYPA|BYPS)\b": "BYP",
                        r"\b(CAMP|CMP)\b": "CP",
                        r"\b(CANYN|CNYN|CANYON|CYN)\b": "CYN",
                        r"\bCAPE\b": "CPE",
                        r"\b(CAUSEWAY|CAUSWA)\b": "CSWY",
                        r"\b(CENT|CEN|CNTR|CNTER|CENTER|CENTR|CENTRE)\b": "CTR",
                        r"\bCHEMIN\b": "CH",
                        r"\b(CTRS|CENTERS)\b": "CTRS",
                        r"\b(CRCLE|CRCL|CIRCL|CIRC|CIRCLE)\b": "CIR",
                        r"\b(CIRCLES|CIRS)\b": "CIRS",
                        r"\bCLIFF\b": "CLF",
                        r"\bCLIFFS\b": "CLFS",
                        r"\bCLUB\b": "CLB",
                        r"\b(CMN|COMMON)\b": "CMN",
                        r"\b(CMNS|COMMONS)\b": "CMNS",
                        r"\bCORNER\b": "COR",
                        r"\bCORNERS\b": "CORS",
                        r"\bCOURSE\b": "CRSE",
                        r"\bCOURT\b": "CT",
                        r"\bCOURTS\b": "CTS",
                        r"\bCOVE\b": "CV",
                        r"\b(COVES|CVS)\b": "CVS",
                        r"\bCREEK\b": "CRK",
                        r"\b(CRSNT|CRSENT|CRESCENT)\b": "CRES",
                        r"\b(CRST|CREST)\b": "CRST",
                        r"\b(CRSSNG|CROSSING)\b": "XING",
                        r"\b(XRD|CROSSROAD)\b": "XRD",
                        r"\b(CROSSROADS|XRDS)\b": "XRDS",
                        r"\b(CURV|CURVE)\b": "CURV",
                        r"\bDALE\b": "DL",
                        r"\bDAM\b": "DM",
                        r"\b(DVD|DIVIDE|DIV)\b": "DV",
                        r"\b(DRIVE|DRIV|DRV)\b": "DR",
                        r"\b(DRS|DRIVES)\b": "DRS",
                        r"\bESTATE\b": "EST",
                        r"\bESTATES\b": "ESTS",
                        r"\b(EXPR|EXPRESSWAY|EXPW|EXPRESS|EXP)\b": "EXPY",
                        r"\b(EXTENSION|EXTNSN|EXTN)\b": "EXT",
                        r"\bEXTS\b": "EXTS",
                        r"\bFALL\b": "FALL",
                        r"\bFALLS\b": "FLS",
                        r"\b(FERRY|FRRY)\b": "FRY",
                        r"\bFIELD\b": "FLD",
                        r"\bFIELDS\b": "FLDS",
                        r"\bFLAT\b": "FLT",
                        r"\bFLATS\b": "FLTS",
                        r"\bFORD\b": "FRD",
                        r"\b(FORDS|FRDS)\b": "FRDS",
                        r"\b(FOREST|FORESTS)\b": "FRST",
                        r"\b(FORG|FORGE)\b": "FRG",
                        r"\b(FORGES|FRGS)\b": "FRGS",
                        r"\bFORK\b": "FRK",
                        r"\bFORKS\b": "FRKS",
                        r"\b(FORT|FRT)\b": "FT",
                        r"\b(FRWY|FREEWAY|FREEWY|FRWAY)\b": "FWY",
                        r"\b(GRDN|GARDEN|GARDN|GDN|GRDEN)\b": "GDN",
                        r"\b(GARDENS|GRDNS)\b": "GDNS",
                        r"\b(GATEWAY|GTWAY|GATEWY|GATWAY)\b": "GTWY",
                        r"\bGLEN\b": "GLN",
                        r"\b(GLNS|GLENS)\b": "GLNS",
                        r"\bGREEN\b": "GRN",
                        r"\b(GREENS|GRNS)\b": "GRNS",
                        r"\b(GROV|GROVE)\b": "GRV",
                        r"\b(GRVS|GROVES)\b": "GRVS",
                        r"\b(HRBOR|HARBR|HARB|HARBOR)\b": "HBR",
                        r"\b(HBRS|HARBORS)\b": "HBRS",
                        r"\bHAVEN\b": "HVN",
                        r"\bHT\b": "HTS",
                        r"\b(HIWAY|HIGHWY|HWAY|HIGHWAY|HIWY)\b": "HWY",
                        r"\bHILL\b": "HL",
                        r"\bHILLS\b": "HLS",
                        r"\b(HLLW|HOLWS|HOLLOWS|HOLLOW)\b": "HOLW",
                        r"\bINLT\b": "INLT",
                        r"\b(ISLND|ISLAND)\b": "IS",
                        r"\b(ISLANDS|ISLNDS)\b": "ISS",
                        r"\bISLES\b": "ISLE",
                        r"\b(JUNCTN|JUNCTON|JCTN|JUNCTION|JCTION)\b": "JCT",
                        r"\b(JCTNS|JUNCTIONS)\b": "JCTS",
                        r"\bKEY\b": "KY",
                        r"\bKEYS\b": "KYS",
                        r"\b(KNOLL|KNOL)\b": "KNL",
                        r"\bKNOLLS\b": "KNLS",
                        r"\bLAKE\b": "LK",
                        r"\bLAKES\b": "LKS",
                        r"\bLAND\b": "LAND",
                        r"\b(LNDNG|LANDING)\b": "LNDG",
                        r"\bLANE\b": "LN",
                        r"\bLIGHT\b": "LGT",
                        r"\b(LIGHTS|LGTS)\b": "LGTS",
                        r"\bLOAF\b": "LF",
                        r"\bLOCK\b": "LCK",
                        r"\bLOCKS\b": "LCKS",
                        r"\b(LODGE|LDGE|LODG)\b": "LDG",
                        r"\bLOOPS\b": "LOOP",
                        r"\bMALL\b": "MALL",
                        r"\bMANOR\b": "MNR",
                        r"\bMANORS\b": "MNRS",
                        r"\b(MEADOW|MDW)\b": "MDW",
                        r"\b(MEDOWS|MDW|MEADOWS)\b": "MDWS",
                        r"\bMEWS\b": "MEWS",
                        r"\b(MILL|ML)\b": "ML",
                        r"\b(MLS|MILLS)\b": "MLS",
                        r"\b(MSN|MSSN|MISSN)\b": "MSN",
                        r"\b(MTWY|MOTORWAY)\b": "MTWY",
                        r"\b(MOUNT|MNT)\b": "MT",
                        r"\b(MNTAIN|MOUNTIN|MOUNTAIN|MNTN|MTIN)\b": "MTN",
                        r"\b(MNTNS|MOUNTAINS|MTNS)\b": "MTNS",
                        r"\bNECK\b": "NCK",
                        r"\b(ORCHARD|ORCHRD)\b": "ORCH",
                        r"\bOVL\b": "OVAL",
                        r"\b(OPAS|OVERPASS)\b": "OPAS",
                        r"\bPRK\b": "PARK",
                        r"\b(PARKS|PARK)\b": "PARK",
                        r"\b(PKWAY|PARKWAY|PARKWY|PKY)\b": "PKWY",
                        r"\b(PARKWAYS|PKWYS|PKWY)\b": "PKWY",
                        r"\bPASS\b": "PASS",
                        r"\b(PASSAGE|PSGE)\b": "PSGE",
                        r"\bPATHS\b": "PATH",
                        r"\bPIKES\b": "PIKE",
                        r"\b(PINE|PNE)\b": "PNE",
                        r"\bPINES\b": "PNES",
                        r"\bPL\b": "PL",
                        r"\bPLAIN\b": "PLN",
                        r"\bPLAINS\b": "PLNS",
                        r"\b(PLAZA|PLZA)\b": "PLZ",
                        r"\bPOINT\b": "PT",
                        r"\bPOINTS\b": "PTS",
                        r"\bPORT\b": "PRT",
                        r"\bPORTS\b": "PRTS",
                        r"\b(PRAIRIE|PRR)\b": "PR",
                        r"\bPROMENADE\b": "PROM",
                        r"\b(RAD|RADIEL|RADIAL)\b": "RADL",
                        r"\bRAMP\b": "RAMP",
                        r"\b(RNCHS|RANCHES|RANCH)\b": "RNCH",
                        r"\bRAPID\b": "RPD",
                        r"\bRAPIDS\b": "RPDS",
                        r"\bREST\b": "RST",
                        r"\b(RIDGE|RDGE)\b": "RDG",
                        r"\bRIDGES\b": "RDGS",
                        r"\b(RIVER|RVR|RIVR)\b": "RIV",
                        r"\bROAD\b": "RD",
                        r"\bROADS\b": "RDS",
                        r"\b(ROUTE|RTE)\b": "RTE",
                        r"\bROW\b": "ROW",
                        r"\bRUE\b": "RUE",
                        r"\bRUN\b": "RUN",
                        r"\bSHOAL\b": "SHL",
                        r"\bSHOALS\b": "SHLS",
                        r"\b(SHOAR|SHORE)\b": "SHR",
                        r"\b(SHOARS|SHORES)\b": "SHRS",
                        r"\b(SKWY|SKYWAY)\b": "SKWY",
                        r"\b(SPRING|SPNG|SPRNG)\b": "SPG",
                        r"\b(SPRINGS|SPNGS|SPRNGS)\b": "SPGS",
                        r"\bSPUR\b": "SPUR",
                        r"\b(SPUR|SPURS)\b": "SPUR",
                        r"\b(SQUARE|SQU|SQR|SQRE)\b": "SQ",
                        r"\b(SQS|SQRS|SQUARES)\b": "SQS",
                        r"\b(STATN|STN|STATION)\b": "STA",
                        r"\b(STRAV|STRVN|STRAVENUE|STRVNUE|STRAVN|STRAVEN)\b": "STRA",
                        r"\b(STREME|STREAM)\b": "STRM",
                        r"\b(STREET|STR|STRT)\b": "ST",
                        r"\b(STS|STREETS)\b": "STS",
                        r"\b(SUMIT|SUMMIT|SUMITT)\b": "SMT",
                        r"\b(TERRACE|TERR)\b": "TER",
                        r"\b(THROUGHWAY|TRWY)\b": "TRWY",
                        r"\b(TRACE|TRACES)\b": "TRCE",
                        r"\b(TRACK|TRK|TRKS|TRACKS)\b": "TRAK",
                        r"\b(TRFY|TRAFFICWAY)\b": "TRFY",
                        r"\b(TRAIL|TRLS|TRAILS)\b": "TRL",
                        r"\b(TRLRS|TRAILER)\b": "TRLR",
                        r"\b(TUNNL|TUNLS|TUNNEL|TUNNELS|TUNEL)\b": "TUNL",
                        r"\b(TPKE|TURNPK|TRNPK|TURNPIKE)\b": "TPKE",
                        r"\b(UPAS|UNDERPASS)\b": "UPAS",
                        r"\bUNION\b": "UN",
                        r"\b(UNIONS|UNS)\b": "UNS",
                        r"\b(VLLY|VALLY|VALLEY)\b": "VLY",
                        r"\bVALLEYS\b": "VLYS",
                        r"\b(VIADCT|VDCT|VIADUCT)\b": "VIA",
                        r"\bVIEW\b": "VW",
                        r"\bVIEWS\b": "VWS",
                        r"\b(VILL|VILLAG|VILLG|VILLAGE|VILLIAGE)\b": "VLG",
                        r"\bVILLAGES\b": "VLGS",
                        r"\bVILLE\b": "VL",
                        r"\b(VSTA|VST|VISTA|VIST)\b": "VIS",
                        r"\bWALK\b": "WALK",
                        r"\b(WALK|WALKS)\b": "WALK",
                        r"\bWALL\b": "WALL",
                        r"\bWY\b": "WAY",
                        r"\bWAYS\b": "WAYS",
                        r"\b(WL|WELL)\b": "WL",
                        r"\bWELLS\b": "WLS"}
                        
                cln = self.str_prep(attribute)
                stz_addresses = cln.replace(ixes, regex=True)

                return stz_addresses

        @safety_switch([list, pd.Series])
        def standardize_postal_code(self, attribute):
                
                """
                Standardizes postal codes 
                
                Args 
                attribute (str) = str containing postal code 
                
                returns str with postal code 
                
                """

                patterns = {r"\.|-|_": "", r"\s{1,}|\n{1,}": "", r"": None}
                
                codes = attribute.replace(patterns, regex=True)
                codes = codes.str.upper().str.strip()
                
                return codes

        @safety_switch([list, pd.Series])
        def standardize_phone_number(self, attribute):
                
                """
                Standardize phone numbers
                
                Args 
                attribute (str) = string of phone number 
                
                returns str with clean telephone num
                
                """
                patterns = {r'\s*|\n*': '', r'\.|\s*|-|_|\n*': '', r'\(|\)':"", r"":None}

                num = attribute.replace(patterns, regex=True)
                num = num.str.upper().str.strip()

                return num

        @safety_switch([list, pd.Series])
        def standardize_province_state(self, attribute):
                """
                Reconcile and convert province, state, or territory titles to abbreviated version
                
                args 
                attribute (str) = str containing either a province or state
                
                returns 
                cleaned string with the short form of the province or states title
                
                """
                patterns = {    r"\bALABAMA\b": "AL",
                                r"\bALASKA\b": "AK", 
                                r"\bAMERICAN SAMOA\b": "AS", 
                                r"\bARIZONA\b": "AZ", 
                                r"\bARKANSAS\b": "AR", 
                                r"\bARMED FORCES AFRICA\b": "AE", 
                                r"\bARMED FORCES AMERICA\b": "AA", 
                                r"\bARMED FORCES CANADA\b": "AE", 
                                r"\bARMED FORCES EUROPE\b": "AE", 
                                r"\bARMED FORCES MIDDLE EAST\b": "AE", 
                                r"\bARMED FORCES PACIFIC\b": "AP", 
                                r"\bCALIFORNIA\b": "CA", 
                                r"\bCOLORADO\b": "CO", 
                                r"\bCONNETICUT\b": "CT", 
                                r"\bDELAWARE\b": "DE", 
                                r"\bDISTRICT OF COLUMBIA\b": "DC", 
                                r"\bFLORIDA\b": "FL", 
                                r"\bGEORGIA\b": "GA", 
                                r"\bGUAM\b": "GU", 
                                r"\bHAWAII\b": "HI", 
                                r"\bIDAHO\b": "ID", 
                                r"\bILLINOIS\b": "IL", 
                                r"\bINDIANA\b": "IN", 
                                r"\bIOWA\b": "IA", 
                                r"\bKANSAS\b": "KS", 
                                r"\bKENTUCKY\b": "KY", 
                                r"\bLOUISIANA\b": "LA", 
                                r"\bMAINE\b": "ME", 
                                r"\bMARSHALL ISLANDS\b": "MH", 
                                r"\bMARYLAND\b": "MD", 
                                r"\bMASSACHUSETTS\b": "MA", 
                                r"\bMICHIGAN\b": "MI", 
                                r"\bMICRONESIA\b": "FM", 
                                r"\bMINNESOTA\b": "MN", 
                                r"\bMINOR OUTLYING ISLANDS\b": "UM", 
                                r"\bMISSISSIPPI\b": "MS", 
                                r"\bMONTANA\b": "MT", 
                                r"\bNEBRASKA\b": "NE", 
                                r"\bNEVADA\b": "NV", 
                                r"\bNEW HAMPSHIRE\b": "NH", 
                                r"\bNEW JERSEY\b": "NJ", 
                                r"\bNEW MEXICO\b": "NM", 
                                r"\bNEW YORK\b": "NY", 
                                r"\bNORTH CAROLINA\b": "NC", 
                                r"\bNORTH DAKOTA\b": "ND", 
                                r"\bNORTHERN MARIANA ISLANDS\b": "MP", 
                                r"\bOHIO\b": "OH", 
                                r"\bOKLAHOMA\b": "OK", 
                                r"\bOREGON\b": "OR", 
                                r"\bPALAU\b": "PW", 
                                r"\bPENNSLYVANIA\b": "PA", 
                                r"\bPUERTO RICO\b": "PR", 
                                r"\bRHODE ISLAND\b": "RI", 
                                r"\bSOUTH CAROLINA\b": "SC", 
                                r"\bSOUTH DAKOTA\b": "SD", 
                                r"\bTENNESSEE\b": "TN", 
                                r"\bTEXAS\b": "TX", 
                                r"\bUTAH\b": "UT", 
                                r"\bVERMONT\b": "VT", 
                                r"\bVIRGINIA\b": "VA", 
                                r"\bVIRGIN ISLANDS\b": "VI", 
                                r"\bWASHINGTON\b": "WA", 
                                r"\bWEST VIRGINIA\b": "WV", 
                                r"\bWISCONSIN\b": "WI", 
                                r"\bWYOMING\b": "WY", 
                                r"\bALBERTA\b": "AB", 
                                r"\bBRITISH COLUMBIA\b": "BC", 
                                r"\bMANITOBA\b": "MB", 
                                r"\bNEW BRUNSWICK\b": "NB", 
                                r"\bNEWFOUNDLAND AND LABRADOR\b": "NL", 
                                r"\bNORTHWEST TERRITORIES\b": "NT", 
                                r"\bNOVA SCOTIA\b": "NS", 
                                r"\bNUNAVUT\b": "NU", 
                                r"\bONTARIO\b|\bONT\b": "ON", 
                                r"\bPRINCE EDWARD ISLAND\b": "PE", 
                                r"\bQUEBEC\b": "QC", 
                                r"\bSASKATCHEWAN\b": "SK", 
                                r"\bYUKON\b": "YT"} 

                cln = self.str_prep(attribute)
                prov_state = pd.Series(cln).replace(patterns, regex=True)        

                return prov_state

        @safety_switch([list, pd.Series])
        def extract_address(self, attribute):
                
                """
                Looks for the basic street address in a string
                street number + street name+ + street direction? 
                
                ARGS
                attribute (pd.Series) a pandas Series containing the strs with the addresses you want to extract
                
                RETURNS 
                list of matched str 

                OTHER
                make sure to pass this through the standardizer -- every prefix and suffix should be uppercase and abbreviated

                note that this has trouble with addresses that contain digits in the middle of them
                
                """
                        
                ixes = re.compile(r"\bN\b|\bS\b|\bE\b|\bW\b|\bNE\b|\bNW\b|\bSE\b|\bSW\b|\bALY\b|\bANX\b|\bARC\b|\bAVE\b|\bBYU\b|\bBCH\b|\bBND\b|\bBLF\b|\bBLFS\b|\bBTM\b|\bBLVD\b|\bBR\b|\bBRG\b|\bBRK\b|\bBRKS\b|\bBG\b|\bBGS\b|\bBYP\b|\bCP\b|\bCYN\b|\bCPE\b|\bCSWY\b|\bCOURS\b|\bCH\b|\bCTR\b|\bCTRS\b|\bCIR\b|\bCIRS\b|\bCLF\b|\bCLFS\b|\bCLB\b|\bCMN\b|\bCMNS\b|\bCOR\b|\bCORS\b|\bCRSE\b|\bCT\b|\bCTS\b|\bCV\b|\bCVS\b|\bCRK\b|\bCRES\b|\bCRST\b|\bXING\b|\bXRD\b|\bXRDS\b|\bCURV\b|\bDL\b|\bDM\b|\bDV\b|\bDR\b|\bDRS\b|\bEST\b|\bESTS\b|\bEXPY\b|\bEXT\b|\bEXTS\b|\bFALL\b|\bFLS\b|\bFRY\b|\bFLD\b|\bFLDS\b|\bFLT\b|\bFLTS\b|\bFRD\b|\bFRDS\b|\bFRST\b|\bFRG\b|\bFRGS\b|\bFRK\b|\bFRKS\b|\bFT\b|\bFWY\b|\bGDN\b|\bGDNS\b|\bGTWY\b|\bGLN\b|\bGLNS\b|\bGRN\b|\bGRNS\b|\bGRV\b|\bGRVS\b|\bHBR\b|\bHBRS\b|\bHVN\b|\bHTS\b|\bHWY\b|\bHL\b|\bHLS\b|\bHOLW\b|\bINLT\b|\bIS\b|\bISS\b|\bISLE\b|\bJCT\b|\bJCTS\b|\bKY\b|\bKYS\b|\bKNL\b|\bKNLS\b|\bLK\b|\bLKS\b|\bLAND\b|\bLNDG\b|\bLN\b|\bLGT\b|\bLGTS\b|\bLF\b|\bLCK\b|\bLCKS\b|\bLDG\b|\bLOOP\b|\bMALL\b|\bMNR\b|\bMNRS\b|\bMDW\b|\bMDWS\b|\bMEWS\b|\bML\b|\bMLS\b|\bMSN\b|\bMTWY\b|\bMT\b|\bMTN\b|\bMTNS\b|\bNCK\b|\bORCH\b|\bOVAL\b|\bOPAS\b|\bPARK\b|\bPARK\b|\bPKWY\b|\bPKWY\b|\bPASS\b|\bPSGE\b|\bPATH\b|\bPIKE\b|\bPNE\b|\bPNES\b|\bPL\b|\bPLN\b|\bPLNS\b|\bPLZ\b|\bPT\b|\bPTS\b|\bPROM\b|\bPRT\b|\bPRTS\b|\bPR\b|\bRADL\b|\bRAMP\b|\bRNCH\b|\bRPD\b|\bRPDS\b|\bRST\b|\bRDG\b|\bRDGS\b|\bRIV\b|\bRD\b|\bRDS\b|\bRTE\b|\bROW\b|\bRUE\b|\bRUN\b|\bSHL\b|\bSHLS\b|\bSHR\b|\bSHRS\b|\bSKWY\b|\bSPG\b|\bSPGS\b|\bSPUR\b|\bSPUR\b|\bSQ\b|\bSQS\b|\bSTA\b|\bSTRA\b|\bSTRM\b|\bST\b|\bSTS\b|\bSMT\b|\bTER\b|\bTRWY\b|\bTRCE\b|\bTRAK\b|\bTRFY\b|\bTRL\b|\bTRLR\b|\bTUNL\b|\bTPKE\b|\bUPAS\b|\bUN\b|\bUNS\b|\bVLY\b|\bVLYS\b|\bVIA\b|\bVW\b|\bVWS\b|\bVLG\b|\bVLGS\b|\bVL\b|\bVIS\b|\bWALK\b|\bWALK\b|\bWALL\b|\bWAY\b|\bWAYS\b|\bWL\b|\bWLS\b")

                address_pattern = re.compile(fr"\b\d+\s[A-Z]*\s({ixes.pattern})?\s?({ixes.pattern})?\s?({ixes.pattern})?")

                search_pattern = list(map(lambda x: x if x == None else address_pattern.search(x), attribute))
                
                pattern_matches = list(map(lambda x: x if x == None else x.group(), search_pattern))

                cln_addy = list(map(lambda x: x if x == None else x.strip(), pattern_matches))
                
                return cln_addy

        @safety_switch([list, pd.Series])
        def extract_postal_code(self, attribute):

                """
                Looks for Canadian and US postal codes / zips
                
                ARGS
                attribute (pd.Series) == a pandas Series containing the strings with possible postal codes that you want to extract
                
                RETURNS 
                matched str 

                OTHER
                make sure to pass this through the standardizer -- everything should be uppercase
                
                """
                
                code_pattern = re.compile(r"\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b|\b\d{5}\b")

                search_pattern = list(map(lambda x: x if x == None else code_pattern.search(x), attribute))
                
                pattern_matches = list(map(lambda x: x if x == None else x.group(), search_pattern))

                return pattern_matches

        @safety_switch([list, pd.Series])
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

                to use function: `df.withColumn('col_name', extract_phone_number(phone_col_inst)) 

                """

                num = re.compile(r"\d\d\d\s?\d\d\d\s?\d\d\d\d\b")

                search_pattern = list(map(lambda x: x if x == None else num.search(x), attribute))

                pattern_matches = list(map(lambda x: x if x == None else x.group(), search_pattern))

                return pattern_matches

