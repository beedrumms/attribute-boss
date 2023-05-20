#%%
import pandas as pd
import numpy as np
import regex as re
## from utils import clean_str (NOT WORKING)
import importlib.util

### TEMP SOLUTION 
temp_path = 'c:/Users/DrummoBre/OneDrive - Government of Ontario/Desktop/SAMS/projects/py_packages/package_zone/AttributeBoss/src/attributeboss/utils.py'
# specify the module that needs to be imported relative to the path of the module
spec = importlib.util.spec_from_file_location("utils", temp_path)
# create a new module based on spec
utils = importlib.util.module_from_spec(spec)
# executes module in its own namespace when modeul is imported or reloaded
spec.loader.exec_module(utils)

#%%
class Standardize:

        def __init__(self):
                print("Initalized - use list(map(lambda x: function(x), attribute)) to iterate through attribute elements")

        def standardize_address(self, _address):
                
                """
                Reconciles and Converts Address Suffix, Prefix, and Street Directions to abbreviated version
                
                Args
                address (str) = a string containing the address
                
                returns str with standard conventions

                note 
                
                
                """

                if _address == None:
                        address = _address
                        
                else: 

                        address = utils.clean_str(_address)

                        ## ENGLISH
                        address = re.sub(r'\bNORTH\b', 'N', address) # north 
                        address = re.sub(r'\bSOUTH\b', 'S', address) # south 
                        address = re.sub(r'\bEAST\b', 'E', address) # east 
                        address = re.sub(r'\bWEST\b', 'W', address) # west

                        address = re.sub(r'\bNORTH\s+EAST\b', 'NE', address)
                        address = re.sub(r'\bNORTH\s+WEST\b', 'NW', address)
                        address = re.sub(r'\bSOUTH\s+EAST\b', 'SE', address)
                        address = re.sub(r'\bSOUTH\s+WEST\b', 'SW', address)

                        address = re.sub(r"\b(ALLY|ALLEE|ALLEY|ALLÉE)\b", "ALY", address)
                        address = re.sub(r"\b(ANNEX|ANNX|ANEX)\b", "ANX", address)
                        address = re.sub(r"\bARCADE\b", "ARC", address)
                        address = re.sub(r"\b(AVNUE|AV|AVENU|AVEN|AVN|AVENUE)\b", "AVE", address)
                        address = re.sub(r"\b(BAYOU|BAYOO|BYU)\b", "BYU", address)
                        address = re.sub(r"\bBEACH\b", "BCH", address)
                        address = re.sub(r"\bBEND\b", "BND", address)
                        address = re.sub(r"\b(BLUF|BLUFF)\b", "BLF", address)
                        address = re.sub(r"\b(BLUFFS|BLFS)\b", "BLFS", address)
                        address = re.sub(r"\b(BOT|BOTTOM|BOTTM)\b", "BTM", address)
                        address = re.sub(r"\b(BOUL|BOULEVARD|BOULV)\b", "BLVD", address)
                        address = re.sub(r"\b(BRANCH|BRNCH)\b", "BR", address)
                        address = re.sub(r"\b(BRDGE|BRIDGE)\b", "BRG", address)
                        address = re.sub(r"\bBROOK\b", "BRK", address)
                        address = re.sub(r"\b(BROOKS|BRKS)\b", "BRKS", address)
                        address = re.sub(r"\b(BURG|BG)\b", "BG", address)
                        address = re.sub(r"\b(BGS|BURGS)\b", "BGS", address)
                        address = re.sub(r"\b(BYPAS|BYPASS|BYPA|BYPS)\b", "BYP", address)
                        address = re.sub(r"\b(CAMP|CMP)\b", "CP", address)
                        address = re.sub(r"\b(CANYN|CNYN|CANYON|CYN)\b", "CYN", address)
                        address = re.sub(r"\bCAPE\b", "CPE", address)
                        address = re.sub(r"\b(CAUSEWAY|CAUSWA)\b", "CSWY", address)
                        address = re.sub(r"\b(CENT|CEN|CNTR|CNTER|CENTER|CENTR|CENTRE)\b", "CTR", address)
                        address = re.sub(r"\bCHEMIN\b", "CH", address)
                        address = re.sub(r"\b(CTRS|CENTERS)\b", "CTRS", address)
                        address = re.sub(r"\b(CRCLE|CRCL|CIRCL|CIRC|CIRCLE)\b", "CIR", address)
                        address = re.sub(r"\b(CIRCLES|CIRS)\b", "CIRS", address)
                        address = re.sub(r"\bCLIFF\b", "CLF", address)
                        address = re.sub(r"\bCLIFFS\b", "CLFS", address)
                        address = re.sub(r"\bCLUB\b", "CLB", address)
                        address = re.sub(r"\b(CMN|COMMON)\b", "CMN", address)
                        address = re.sub(r"\b(CMNS|COMMONS)\b", "CMNS", address)
                        address = re.sub(r"\bCORNER\b", "COR", address)
                        address = re.sub(r"\bCORNERS\b", "CORS", address)
                        address = re.sub(r"\bCOURSE\b", "CRSE", address)
                        address = re.sub(r"\bCOURT\b", "CT", address)
                        address = re.sub(r"\bCOURTS\b", "CTS", address)
                        address = re.sub(r"\bCOVE\b", "CV", address)
                        address = re.sub(r"\b(COVES|CVS)\b", "CVS", address)
                        address = re.sub(r"\bCREEK\b", "CRK", address)
                        address = re.sub(r"\b(CRSNT|CRSENT|CRESCENT)\b", "CRES", address)
                        address = re.sub(r"\b(CRST|CREST)\b", "CRST", address)
                        address = re.sub(r"\b(CRSSNG|CROSSING)\b", "XING", address)
                        address = re.sub(r"\b(XRD|CROSSROAD)\b", "XRD", address)
                        address = re.sub(r"\b(CROSSROADS|XRDS)\b", "XRDS", address)
                        address = re.sub(r"\b(CURV|CURVE)\b", "CURV", address)
                        address = re.sub(r"\bDALE\b", "DL", address)
                        address = re.sub(r"\bDAM\b", "DM", address)
                        address = re.sub(r"\b(DVD|DIVIDE|DIV)\b", "DV", address)
                        address = re.sub(r"\b(DRIVE|DRIV|DRV)\b", "DR", address)
                        address = re.sub(r"\b(DRS|DRIVES)\b", "DRS", address)
                        address = re.sub(r"\bESTATE\b", "EST", address)
                        address = re.sub(r"\bESTATES\b", "ESTS", address)
                        address = re.sub(r"\b(EXPR|EXPRESSWAY|EXPW|EXPRESS|EXP)\b", "EXPY", address)
                        address = re.sub(r"\b(EXTENSION|EXTNSN|EXTN)\b", "EXT", address)
                        address = re.sub(r"\bEXTS\b", "EXTS", address)
                        address = re.sub(r"\bFALL\b", "FALL", address)
                        address = re.sub(r"\bFALLS\b", "FLS", address)
                        address = re.sub(r"\b(FERRY|FRRY)\b", "FRY", address)
                        address = re.sub(r"\bFIELD\b", "FLD", address)
                        address = re.sub(r"\bFIELDS\b", "FLDS", address)
                        address = re.sub(r"\bFLAT\b", "FLT", address)
                        address = re.sub(r"\bFLATS\b", "FLTS", address)
                        address = re.sub(r"\bFORD\b", "FRD", address)
                        address = re.sub(r"\b(FORDS|FRDS)\b", "FRDS", address)
                        address = re.sub(r"\b(FOREST|FORESTS)\b", "FRST", address)
                        address = re.sub(r"\b(FORG|FORGE)\b", "FRG", address)
                        address = re.sub(r"\b(FORGES|FRGS)\b", "FRGS", address)
                        address = re.sub(r"\bFORK\b", "FRK", address)
                        address = re.sub(r"\bFORKS\b", "FRKS", address)
                        address = re.sub(r"\b(FORT|FRT)\b", "FT", address)
                        address = re.sub(r"\b(FRWY|FREEWAY|FREEWY|FRWAY)\b", "FWY", address)
                        address = re.sub(r"\b(GRDN|GARDEN|GARDN|GDN|GRDEN)\b", "GDN", address)
                        address = re.sub(r"\b(GARDENS|GRDNS)\b", "GDNS", address)
                        address = re.sub(r"\b(GATEWAY|GTWAY|GATEWY|GATWAY)\b", "GTWY", address)
                        address = re.sub(r"\bGLEN\b", "GLN", address)
                        address = re.sub(r"\b(GLNS|GLENS)\b", "GLNS", address)
                        address = re.sub(r"\bGREEN\b", "GRN", address)
                        address = re.sub(r"\b(GREENS|GRNS)\b", "GRNS", address)
                        address = re.sub(r"\b(GROV|GROVE)\b", "GRV", address)
                        address = re.sub(r"\b(GRVS|GROVES)\b", "GRVS", address)
                        address = re.sub(r"\b(HRBOR|HARBR|HARB|HARBOR)\b", "HBR", address)
                        address = re.sub(r"\b(HBRS|HARBORS)\b", "HBRS", address)
                        address = re.sub(r"\bHAVEN\b", "HVN", address)
                        address = re.sub(r"\bHT\b", "HTS", address)
                        address = re.sub(r"\b(HIWAY|HIGHWY|HWAY|HIGHWAY|HIWY)\b", "HWY", address)
                        address = re.sub(r"\bHILL\b", "HL", address)
                        address = re.sub(r"\bHILLS\b", "HLS", address)
                        address = re.sub(r"\b(HLLW|HOLWS|HOLLOWS|HOLLOW)\b", "HOLW", address)
                        address = re.sub(r"\bINLT\b", "INLT", address)
                        address = re.sub(r"\b(ISLND|ISLAND)\b", "IS", address)
                        address = re.sub(r"\b(ISLANDS|ISLNDS)\b", "ISS", address)
                        address = re.sub(r"\bISLES\b", "ISLE", address)
                        address = re.sub(r"\b(JUNCTN|JUNCTON|JCTN|JUNCTION|JCTION)\b", "JCT", address)
                        address = re.sub(r"\b(JCTNS|JUNCTIONS)\b", "JCTS", address)
                        address = re.sub(r"\bKEY\b", "KY", address)
                        address = re.sub(r"\bKEYS\b", "KYS", address)
                        address = re.sub(r"\b(KNOLL|KNOL)\b", "KNL", address)
                        address = re.sub(r"\bKNOLLS\b", "KNLS", address)
                        address = re.sub(r"\bLAKE\b", "LK", address)
                        address = re.sub(r"\bLAKES\b", "LKS", address)
                        address = re.sub(r"\bLAND\b", "LAND", address)
                        address = re.sub(r"\b(LNDNG|LANDING)\b", "LNDG", address)
                        address = re.sub(r"\bLANE\b", "LN", address)
                        address = re.sub(r"\bLIGHT\b", "LGT", address)
                        address = re.sub(r"\b(LIGHTS|LGTS)\b", "LGTS", address)
                        address = re.sub(r"\bLOAF\b", "LF", address)
                        address = re.sub(r"\bLOCK\b", "LCK", address)
                        address = re.sub(r"\bLOCKS\b", "LCKS", address)
                        address = re.sub(r"\b(LODGE|LDGE|LODG)\b", "LDG", address)
                        address = re.sub(r"\bLOOPS\b", "LOOP", address)
                        address = re.sub(r"\bMALL\b", "MALL", address)
                        address = re.sub(r"\bMANOR\b", "MNR", address)
                        address = re.sub(r"\bMANORS\b", "MNRS", address)
                        address = re.sub(r"\b(MEADOW|MDW)\b", "MDW", address)
                        address = re.sub(r"\b(MEDOWS|MDW|MEADOWS)\b", "MDWS", address)
                        address = re.sub(r"\bMEWS\b", "MEWS", address)
                        address = re.sub(r"\b(MILL|ML)\b", "ML", address)
                        address = re.sub(r"\b(MLS|MILLS)\b", "MLS", address)
                        address = re.sub(r"\b(MSN|MSSN|MISSN)\b", "MSN", address)
                        address = re.sub(r"\b(MTWY|MOTORWAY)\b", "MTWY", address)
                        address = re.sub(r"\b(MOUNT|MNT)\b", "MT", address)
                        address = re.sub(r"\b(MNTAIN|MOUNTIN|MOUNTAIN|MNTN|MTIN)\b", "MTN", address)
                        address = re.sub(r"\b(MNTNS|MOUNTAINS|MTNS)\b", "MTNS", address)
                        address = re.sub(r"\bNECK\b", "NCK", address)
                        address = re.sub(r"\b(ORCHARD|ORCHRD)\b", "ORCH", address)
                        address = re.sub(r"\bOVL\b", "OVAL", address)
                        address = re.sub(r"\b(OPAS|OVERPASS)\b", "OPAS", address)
                        address = re.sub(r"\bPRK\b", "PARK", address)
                        address = re.sub(r"\b(PARKS|PARK)\b", "PARK", address)
                        address = re.sub(r"\b(PKWAY|PARKWAY|PARKWY|PKY)\b", "PKWY", address)
                        address = re.sub(r"\b(PARKWAYS|PKWYS|PKWY)\b", "PKWY", address)
                        address = re.sub(r"\bPASS\b", "PASS", address)
                        address = re.sub(r"\b(PASSAGE|PSGE)\b", "PSGE", address)
                        address = re.sub(r"\bPATHS\b", "PATH", address)
                        address = re.sub(r"\bPIKES\b", "PIKE", address)
                        address = re.sub(r"\b(PINE|PNE)\b", "PNE", address)
                        address = re.sub(r"\bPINES\b", "PNES", address)
                        address = re.sub(r"\bPL\b", "PL", address)
                        address = re.sub(r"\bPLAIN\b", "PLN", address)
                        address = re.sub(r"\bPLAINS\b", "PLNS", address)
                        address = re.sub(r"\b(PLAZA|PLZA)\b", "PLZ", address)
                        address = re.sub(r"\bPOINT\b", "PT", address)
                        address = re.sub(r"\bPOINTS\b", "PTS", address)
                        address = re.sub(r"\bPORT\b", "PRT", address)
                        address = re.sub(r"\bPORTS\b", "PRTS", address)
                        address = re.sub(r"\b(PRAIRIE|PRR)\b", "PR", address)
                        address = re.sub(r"\bPROMENADE\b", "PROM", address)
                        address = re.sub(r"\b(RAD|RADIEL|RADIAL)\b", "RADL", address)
                        address = re.sub(r"\bRAMP\b", "RAMP", address)
                        address = re.sub(r"\b(RNCHS|RANCHES|RANCH)\b", "RNCH", address)
                        address = re.sub(r"\bRAPID\b", "RPD", address)
                        address = re.sub(r"\bRAPIDS\b", "RPDS", address)
                        address = re.sub(r"\bREST\b", "RST", address)
                        address = re.sub(r"\b(RIDGE|RDGE)\b", "RDG", address)
                        address = re.sub(r"\bRIDGES\b", "RDGS", address)
                        address = re.sub(r"\b(RIVER|RVR|RIVR)\b", "RIV", address)
                        address = re.sub(r"\bROAD\b", "RD", address)
                        address = re.sub(r"\bROADS\b", "RDS", address)
                        address = re.sub(r"\b(ROUTE|RTE)\b", "RTE", address)
                        address = re.sub(r"\bROW\b", "ROW", address)
                        address = re.sub(r"\bRUE\b", "RUE", address)
                        address = re.sub(r"\bRUN\b", "RUN", address)
                        address = re.sub(r"\bSHOAL\b", "SHL", address)
                        address = re.sub(r"\bSHOALS\b", "SHLS", address)
                        address = re.sub(r"\b(SHOAR|SHORE)\b", "SHR", address)
                        address = re.sub(r"\b(SHOARS|SHORES)\b", "SHRS", address)
                        address = re.sub(r"\b(SKWY|SKYWAY)\b", "SKWY", address)
                        address = re.sub(r"\b(SPRING|SPNG|SPRNG)\b", "SPG", address)
                        address = re.sub(r"\b(SPRINGS|SPNGS|SPRNGS)\b", "SPGS", address)
                        address = re.sub(r"\bSPUR\b", "SPUR", address)
                        address = re.sub(r"\b(SPUR|SPURS)\b", "SPUR", address)
                        address = re.sub(r"\b(SQUARE|SQU|SQR|SQRE)\b", "SQ", address)
                        address = re.sub(r"\b(SQS|SQRS|SQUARES)\b", "SQS", address)
                        address = re.sub(r"\b(STATN|STN|STATION)\b", "STA", address)
                        address = re.sub(r"\b(STRAV|STRVN|STRAVENUE|STRVNUE|STRAVN|STRAVEN)\b", "STRA", address)
                        address = re.sub(r"\b(STREME|STREAM)\b", "STRM", address)
                        address = re.sub(r"\b(STREET|STR|STRT)\b", "ST", address)
                        address = re.sub(r"\b(STS|STREETS)\b", "STS", address)
                        address = re.sub(r"\b(SUMIT|SUMMIT|SUMITT)\b", "SMT", address)
                        address = re.sub(r"\b(TERRACE|TERR)\b", "TER", address)
                        address = re.sub(r"\b(THROUGHWAY|TRWY)\b", "TRWY", address)
                        address = re.sub(r"\b(TRACE|TRACES)\b", "TRCE", address)
                        address = re.sub(r"\b(TRACK|TRK|TRKS|TRACKS)\b", "TRAK", address)
                        address = re.sub(r"\b(TRFY|TRAFFICWAY)\b", "TRFY", address)
                        address = re.sub(r"\b(TRAIL|TRLS|TRAILS)\b", "TRL", address)
                        address = re.sub(r"\b(TRLRS|TRAILER)\b", "TRLR", address)
                        address = re.sub(r"\b(TUNNL|TUNLS|TUNNEL|TUNNELS|TUNEL)\b", "TUNL", address)
                        address = re.sub(r"\b(TPKE|TURNPK|TRNPK|TURNPIKE)\b", "TPKE", address)
                        address = re.sub(r"\b(UPAS|UNDERPASS)\b", "UPAS", address)
                        address = re.sub(r"\bUNION\b", "UN", address)
                        address = re.sub(r"\b(UNIONS|UNS)\b", "UNS", address)
                        address = re.sub(r"\b(VLLY|VALLY|VALLEY)\b", "VLY", address)
                        address = re.sub(r"\bVALLEYS\b", "VLYS", address)
                        address = re.sub(r"\b(VIADCT|VDCT|VIADUCT)\b", "VIA", address)
                        address = re.sub(r"\bVIEW\b", "VW", address)
                        address = re.sub(r"\bVIEWS\b", "VWS", address)
                        address = re.sub(r"\b(VILL|VILLAG|VILLG|VILLAGE|VILLIAGE)\b", "VLG", address)
                        address = re.sub(r"\bVILLAGES\b", "VLGS", address)
                        address = re.sub(r"\bVILLE\b", "VL", address)
                        address = re.sub(r"\b(VSTA|VST|VISTA|VIST)\b", "VIS", address)
                        address = re.sub(r"\bWALK\b", "WALK", address)
                        address = re.sub(r"\b(WALK|WALKS)\b", "WALK", address)
                        address = re.sub(r"\bWALL\b", "WALL", address)
                        address = re.sub(r"\bWY\b", "WAY", address)
                        address = re.sub(r"\bWAYS\b", "WAYS", address)
                        address = re.sub(r"\b(WL|WELL)\b", "WL", address)
                        address = re.sub(r"\bWELLS\b", "WLS", address)
                
                return address

        #%%

        def standardize_postal_code(self, postal):
                
                """
                Standardizes postal codes 
                
                Args 
                postal (str) = str containing postal code 
                
                returns str with postal code 
                
                """

                if postal == None:
                        code = postal

                else: 
                        
                        m = re.findall(r"\b[A-Za-z]\d[A-Za-z]\s?-?\d[A-Za-z]\d\b", str(postal))

                        if m:
                                p = re.sub(r'\s*', '', m[0])
                                p = re.sub(r'[.\\-_]', '', p)
                                pp = p.upper().strip()
                                code = re.sub(r"\b[A-Za-z]\d[A-Za-z](\s?|-{1})\d[A-Za-z]\d\b", fr"{pp}", postal)
                        else:
                                code = postal

                return code

        #%% 

        def standardize_phone_number(self, number):
                
                """
                Standardize phone numbers
                
                Args 
                number (str) = string of phone number 
                
                returns str with clean telephone num
                
                """

                if number == None:
                        num = number
                else:
                        num = re.sub(r'\s*|\n*', '', str(number))
                        num = re.sub(r'\.|\s*|-|_|\n*', '', num)

                return num

        # %%
        def standardize_province_state(self, province_state):
                """
                Reconcile and convert province, state, or territory titles to abbreviated version
                
                args 
                prov_state (str) = str containing either a province or state
                
                returns 
                cleaned string with the short form of the province or states title
                
                """

                if province_state == None:
                        prov_state = province_state
                else:
                        prov_state = utils.clean_str(province_state)

                        prov_state = re.sub(r"\bALABAMA\b", "AL", prov_state)
                        prov_state = re.sub(r"\bALASKA\b", "AK", prov_state)
                        prov_state = re.sub(r"\bAMERICAN SAMOA\b", "AS", prov_state)
                        prov_state = re.sub(r"\bARIZONA\b", "AZ", prov_state)
                        prov_state = re.sub(r"\bARKANSAS\b", "AR", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES AFRICA\b", "AE", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES AMERICA\b", "AA", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES CANADA\b", "AE", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES EUROPE\b", "AE", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES MIDDLE EAST\b", "AE", prov_state)
                        prov_state = re.sub(r"\bARMED FORCES PACIFIC\b", "AP", prov_state)
                        prov_state = re.sub(r"\bCALIFORNIA\b", "CA", prov_state)
                        prov_state = re.sub(r"\bCOLORADO\b", "CO", prov_state)
                        prov_state = re.sub(r"\bCONNETICUT\b", "CT", prov_state)
                        prov_state = re.sub(r"\bDELAWARE\b", "DE", prov_state)
                        prov_state = re.sub(r"\bDISTRICT OF COLUMBIA\b", "DC", prov_state)
                        prov_state = re.sub(r"\bFLORIDA\b", "FL", prov_state)
                        prov_state = re.sub(r"\bGEORGIA\b", "GA", prov_state)
                        prov_state = re.sub(r"\bGUAM\b", "GU", prov_state)
                        prov_state = re.sub(r"\bHAWAII\b", "HI", prov_state)
                        prov_state = re.sub(r"\bIDAHO\b", "ID", prov_state)
                        prov_state = re.sub(r"\bILLINOIS\b", "IL", prov_state)
                        prov_state = re.sub(r"\bINDIANA\b", "IN", prov_state)
                        prov_state = re.sub(r"\bIOWA\b", "IA", prov_state)
                        prov_state = re.sub(r"\bKANSAS\b", "KS", prov_state)
                        prov_state = re.sub(r"\bKENTUCKY\b", "KY", prov_state)
                        prov_state = re.sub(r"\bLOUISIANA\b", "LA", prov_state)
                        prov_state = re.sub(r"\bMAINE\b", "ME", prov_state)
                        prov_state = re.sub(r"\bMARSHALL ISLANDS\b", "MH", prov_state)
                        prov_state = re.sub(r"\bMARYLAND\b", "MD", prov_state)
                        prov_state = re.sub(r"\bMASSACHUSETTS\b", "MA", prov_state)
                        prov_state = re.sub(r"\bMICHIGAN\b", "MI", prov_state)
                        prov_state = re.sub(r"\bMICRONESIA\b", "FM", prov_state)
                        prov_state = re.sub(r"\bMINNESOTA\b", "MN", prov_state)
                        prov_state = re.sub(r"\bMINOR OUTLYING ISLANDS\b", "UM", prov_state)
                        prov_state = re.sub(r"\bMISSISSIPPI\b", "MS", prov_state)
                        prov_state = re.sub(r"\bMONTANA\b", "MT", prov_state)
                        prov_state = re.sub(r"\bNEBRASKA\b", "NE", prov_state)
                        prov_state = re.sub(r"\bNEVADA\b", "NV", prov_state)
                        prov_state = re.sub(r"\bNEW HAMPSHIRE\b", "NH", prov_state)
                        prov_state = re.sub(r"\bNEW JERSEY\b", "NJ", prov_state)
                        prov_state = re.sub(r"\bNEW MEXICO\b", "NM", prov_state)
                        prov_state = re.sub(r"\bNEW YORK\b", "NY", prov_state)
                        prov_state = re.sub(r"\bNORTH CAROLINA\b", "NC", prov_state)
                        prov_state = re.sub(r"\bNORTH DAKOTA\b", "ND", prov_state)
                        prov_state = re.sub(r"\bNORTHERN MARIANA ISLANDS\b", "MP", prov_state)
                        prov_state = re.sub(r"\bOHIO\b", "OH", prov_state)
                        prov_state = re.sub(r"\bOKLAHOMA\b", "OK", prov_state)
                        prov_state = re.sub(r"\bOREGON\b", "OR", prov_state)
                        prov_state = re.sub(r"\bPALAU\b", "PW", prov_state)
                        prov_state = re.sub(r"\bPENNSLYVANIA\b", "PA", prov_state)
                        prov_state = re.sub(r"\bPUERTO RICO\b", "PR", prov_state)
                        prov_state = re.sub(r"\bRHODE ISLAND\b", "RI", prov_state)
                        prov_state = re.sub(r"\bSOUTH CAROLINA\b", "SC", prov_state)
                        prov_state = re.sub(r"\bSOUTH DAKOTA\b", "SD", prov_state)
                        prov_state = re.sub(r"\bTENNESSEE\b", "TN", prov_state)
                        prov_state = re.sub(r"\bTEXAS\b", "TX", prov_state)
                        prov_state = re.sub(r"\bUTAH\b", "UT", prov_state)
                        prov_state = re.sub(r"\bVERMONT\b", "VT", prov_state)
                        prov_state = re.sub(r"\bVIRGINIA\b", "VA", prov_state)
                        prov_state = re.sub(r"\bVIRGIN ISLANDS\b", "VI", prov_state)
                        prov_state = re.sub(r"\bWASHINGTON\b", "WA", prov_state)
                        prov_state = re.sub(r"\bWEST VIRGINIA\b", "WV", prov_state)
                        prov_state = re.sub(r"\bWISCONSIN\b", "WI", prov_state)
                        prov_state = re.sub(r"\bWYOMING\b", "WY", prov_state)
                        prov_state = re.sub(r"\bALBERTA\b", "AB", prov_state)
                        prov_state = re.sub(r"\bBRITISH COLUMBIA\b", "BC", prov_state)
                        prov_state = re.sub(r"\bMANITOBA\b", "MB", prov_state)
                        prov_state = re.sub(r"\bNEW BRUNSWICK\b", "NB", prov_state)
                        prov_state = re.sub(r"\bNEWFOUNDLAND AND LABRADOR\b", "NL", prov_state)
                        prov_state = re.sub(r"\bNORTHWEST TERRITORIES\b", "NT", prov_state)
                        prov_state = re.sub(r"\bNOVA SCOTIA\b", "NS", prov_state)
                        prov_state = re.sub(r"\bPROVINCE OR TERRITORY\b", "Abbreviation", prov_state)
                        prov_state = re.sub(r"\bNUNAVUT\b", "NU", prov_state)
                        prov_state = re.sub(r"\bONTARIO\b", "ON", prov_state)
                        prov_state = re.sub(r"\bPRINCE EDWARD ISLAND\b", "PE", prov_state)
                        prov_state = re.sub(r"\bQUEBEC\b", "QC", prov_state)
                        prov_state = re.sub(r"\bSASKATCHEWAN\b", "SK", prov_state)
                        prov_state = re.sub(r"\bYUKON\b", "YT", prov_state)

                return prov_state


# %%
