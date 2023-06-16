#%%
import pandas as pd
import numpy as np
import regex as re

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from features import clean_strs 
###

#%%
class Standardize:

        def __init__(self):
                print("Initalized - use list(map(lambda x: function(x), attribute)) to iterate through attribute elements")

        #%%
        def standardize_address(self, addresses):

                """
                Converts str to upper,  removes any double+ spaces, hyphens non-ASCII characters, apostrophes, periods, parenthesis, replaces french accents, strips trailing ws
                
                ARGS
                addresses (pd.Series / list) = a pd.Series or list containing address data you want to standardize
                
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

                try:

                        cln = clean_strs(addresses)
                        stz_addresses = pd.Series(cln).replace(ixes, regex=True)

                        return stz_addresses

                except AttributeError as e: 

                        print("Missing required argument or incorrect data struct given for arg *address*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

                except TypeError as e:

                        print("Incorrect data given for arg *address*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

        #%%

        def standardize_postal_code(self, postal):
                
                """
                Standardizes postal codes 
                
                Args 
                postal (str) = str containing postal code 
                
                returns str with postal code 
                
                """

                patterns = {r"\.|-|_": "", r"\s{1,}|\n{1,}": "", r"": None}

                try:
                        codes = pd.Series(postal).replace(patterns, regex=True)
                        codes = codes.str.upper().str.strip()
                        
                        return codes

                except AttributeError as e: 

                        print("Missing required argument or incorrect data struct given for arg *postal*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

                except TypeError as e:

                        print("Incorrect data given for arg *postal*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

        #%% 
        def standardize_phone_number(self, phone_number):
                
                """
                Standardize phone numbers
                
                Args 
                number (str) = string of phone number 
                
                returns str with clean telephone num
                
                """
                patterns = {r'\s*|\n*': '', r'\.|\s*|-|_|\n*': '', r'\(|\)':"", r"":None}

                try:
                        num = pd.Series(phone_number).replace(patterns, regex=True)
                        num = num.str.upper().str.strip()
                        return num

                except AttributeError as e: 

                        print("Missing required argument or incorrect data struct given for arg *phone_number*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

                except TypeError as e:

                        print("Incorrect data given for arg *phone_number*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

        # %%
        def standardize_province_state(self, province_state):
                """
                Reconcile and convert province, state, or territory titles to abbreviated version
                
                args 
                prov_state (str) = str containing either a province or state
                
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

                try:

                        cln = clean_strs(province_state)
                        prov_state = pd.Series(cln).replace(patterns, regex=True)        

                        return prov_state

                except AttributeError as e: 

                        print("Missing required argument or incorrect data struct given for arg *prov_state*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise

                except TypeError as e:

                        print("Incorrect data given for arg *prov_state*", "\n", "see func.__doc__ for more information on use", "\n",  str(e))
                        raise


# %%
