#%%
import pandas as pd
import numpy as np
import regex as re

#%% 
def FeaturesTestingData(): 

    arr = np.array([["", "Aycan", "401 Smyth Rd, Children's Hospital of Eastern Ontario (CHEO)",
    None, "AYCAN", "401 SMYTH RD, CHILDRENS HOSPITAL OF EASTERN ONTARIO CHEO"],

    ["NONE", "Iskhakova   ", "800 Commissioners Rd.E, PO box 5010, Station B, LHSC, Department of Medicine/Schulich,, C6-400",None, "ISKHAKOVA", "800 COMMISSIONERS RD E, PO BOX 5010, STATION B, LHSC, DEPARTMENT OF MEDICINE/SCHULICH, C6 400"],

    [np.nan, "  Fabian", "The Montfort Hospital, 713 Montreal Road  ",
    None, "FABIAN", "THE MONTFORT HOSPITAL, 713 MONTREAL ROAD"],

    ["NA", "Blackmoré", "Quite Health Care, Trenton Memorial Hospital, 242 King Avenue Street e",
    None, "BLACKMORE", "QUITE HEALTH CARE, TRENTON MEMORIAL HOSPITAL, 242 KING AVENUE STREET E"],

    ["", "Pokrupà-Nahanni", "19 Hospital Road street north west, P.O. Box 664",
    None, "POKRUPA NAHANNI", "19 HOSPITAL ROAD STREET NORTH WEST, PO BOX 664"],
    
    ['', " penpen", "  Suite 209, 5293 Highway 7 East",
    None, "PENPEN", "SUITE 209, 5293 HIGHWAY 7 EAST"], 

    ["na", "Schnarr", "HHS St Peter's Hospital, 88 Maplewood Ave",None, "SCHNARR", "HHS ST PETERS HOSPITAL, 88 MAPLEWOOD AVE"], 

    ["none", "Peticca ", "  LMC Ottawa Diabetes & Endocrinology, 208-4100 Strandherd Drive South West",
    None, "PETICCA", "LMC OTTAWA DIABETES & ENDOCRINOLOGY, 208 4100 STRANDHERD DRIVE SOUTH WEST"],

    ["None", "boB", "Unit 311 , 15 Mountain Avenue South",
    None, "BOB", "UNIT 311, 15 MOUNTAIN AVENUE SOUTH"],

    ["Na", "Abdallah”", "University of Ottawa, Department of Anesthesiology, and Pain Medicine, 1053 Carling Ave road s",None, "ABDALLAH", "UNIVERSITY OF OTTAWA, DEPARTMENT OF ANESTHESIOLOGY, AND PAIN MEDICINE, 1053 CARLING AVE ROAD S"],

    [np.nan, " Papnéjà", "Iona Doctors     Clinic   , Iona Plaza, 1585 Mississauga Valley Boulevard",
    None, "PAPNEJA", "IONA DOCTORS CLINIC, IONA PLAZA, 1585 MISSISSAUGA VALLEY BOULEVARD"], 

    ["YAY", "Reyes¢", "Ste 300-301, 40 Holly Street",
    "YAY", "REYES", "STE 300 301, 40 HOLLY STREET"]]) 

    t_arr = arr.transpose()

    frame = pd.DataFrame(t_arr, ["Nulls_actual", "Name_actual", "Address_actual", "Nulls_expected",  "Name_expected", "Address_expected"])

    df = frame.transpose()

    print(type(df), " test data created")
    
    return df 

###
#%%
def StandardizeTestingData(): 
    
    arr = np.array([["401 Smyth Road, Children's Hospital of Eastern Ontario CHEO)",
    "401 SMYTH RD, CHILDRENS HOSPITAL OF EASTERN ONTARIO CHEO",
    " M 4b2 J8", 
    "M4B2J8",
    9053020123,
    "9053020123",
    "Alabama",
    "AL"],

    ["800 Commissioners Road.E, PO box 5010, Station B, LHSC, Department of Medicine/Schulich,, C6-400",
    "800 COMMISSIONERS RD E, PO BOX 5010, STA B, LHSC, DEPARTMENT OF MEDICINE/SCHULICH, C6 400",
    "k0c0c3  ",
    "K0C0C3",
    "1(905)-853-1304",
    "19058531304",
    "ont",
    "ON"],

    ["The Montfort Hospital, 713 Montreal street west  ",
    "THE MONTFORT HOSPITAL, 713 MONTREAL ST W",
    "N8N  0b3",
    "N8N0B3",
    "1800-123-1349",
    "18001231349",
    "ontario",
    "ON"],

    ["Quite Health Care, Trenton Memorial Hospital, 242 King Avenue Street e",
    "QUITE HEALTH CARE, TRENTON MEMORIAL HOSPITAL, 242 KING AVE ST E",
    "L8E-1A1",
    "L8E1A1",
    "(906)123-1304",
    "9061231304",
    "quebec",
    "QC"],

    ["19 Hospital Road street north west, P.O. Box 664",
    "19 HOSPITAL RD ST NW, PO BOX 664",
    "P3A--0A1",
    "P3A0A1",
    "230.031.1230",
    "2300311230",
    "on",
    "ON"],
    
    ["  Suite 209, 5293 Highway 7 East",
    "SUITE 209, 5293 HWY 7 E",
    "L5C 2v3",
    "L5C2V3",
    "503-1032",
    "5031032",
    "washington",
    "WA"], 

    ["HHS St Peter's Hospital, 88 Maplewood Ave Road southwest",
    "HHS ST PETERS HOSPITAL, 88 MAPLEWOOD AVE RD SW",
    "k0g0a7",
    "K0G0A7",
    "1-(233)-241-5235",
    "12332415235",
    "British Columbia",
    "BC"], 

    ["  LMC Ottawa Diabetes & Endocrinology, 208-4100 Strandherd Drive South West",
    "LMC OTTAWA DIABETES & ENDOCRINOLOGY, 208 4100 STRANDHERD DR SW",
    "P3A1-E2",
    "P3A1E2",
    "123.123.1234",
    "1231231234",
    "alberta",
    "AB"],

    ["Unit 311 , 15 Mountain Avenue CRESCENT South",
    "UNIT 311, 15 MTN AVE CRES S",
    "K 7 K 0 J 2",
    "K7K0J2",
    "416 145 1556",
    "4161451556",
    "MANITOBA",
    "MB"],

    ["University of Ottawa, Department of Anesthesiology, and Pain Medicine, 1053 Carling Ave road s",
    "UNIVERSITY OF OTTAWA, DEPARTMENT OF ANESTHESIOLOGY, AND PAIN MEDICINE, 1053 CARLING AVE RD S",
    "N8N0B3   ",
    "N8N0B3",
    "456  933 2223",
    "4569332223",
    "NEW BRUNSWICK",
    "NB"],

    ["Iona Doctors     Clinic   , Iona Plaza, 1585 Mississauga Valley Boul.",
    "IONA DOCTORS CLINIC, IONA PLZ, 1585 MISSISSAUGA VLY BLVD",
    "K0A-0A2",
    "K0A0A2",
    "(842)-234-2345",
    "8422342345",
    "SASKATCHEWAN",
    "SK"], 

    ["Ste 300-301, 40 Holly Street",
    "STE 300 301, 40 HOLLY ST",
    "M5R2P3",
    "M5R2P3",
    "432-123-0194",
    "4321230194",
    "NORTHWEST TERRITORIES",
    "NT"]]) 

    t_arr = arr.transpose()

    frame = pd.DataFrame(t_arr, ["Address_actual", "Address_expected", "Postal_actual", "Postal_expected", "Phone_actual", "Phone_expected", "Prov_actual", "Prov_expected"])

    df = frame.transpose()

    print(type(df), " test data created")
    
    return df 

# %%
def ExtractTestingData(): 
    
    arr = np.array([["401 SMYTH RD, CHILDRENS HOSPITAL OF EASTERN ONTARIO CHEO M4B2J8 9053020123",
    "401 SMYTH RD",
    "M4B2J8",
    "9053020123"],

    ["800 COMMISSIONERS RD E, PO BOX 5010, STA B, LHSC, DEPARTMENT OF MEDICINE/SCHULICH, C6 400 K0C0C3 19058531304",
    "800 COMMISSIONERS RD E",
    "K0C0C3",
    "9058531304"],

    ["THE MONTFORT HOSPITAL, 713 MONTREAL ST W N8N0B3 18001231349",
    "713 MONTREAL ST W",
    "N8N0B3",
    "8001231349"],

    ["QUITE HEALTH CARE, TRENTON MEMORIAL HOSPITAL, 242 KING AVE ST E L8E1A1 9061231304",
    "242 KING AVE ST E",
    "L8E1A1",
    "9061231304"],

    ["19 HOSPITAL RD ST NW, PO BOX 664 P3A0A1 2300311230",
    "19 HOSPITAL RD ST NW",
    "P3A0A1",
    "2300311230"],
    
    ["SUITE 209, 5293 HWY 7 E L5C2V3 5031032",
    "5293 HWY",
    "L5C2V3",
    None], 

    ["HHS ST PETERS HOSPITAL, 88 MAPLEWOOD AVE RD SW K0G0A7 12332415235",
    "88 MAPLEWOOD AVE RD SW",
    "K0G0A7",
    "2332415235"], 

    ["LMC OTTAWA DIABETES & ENDOCRINOLOGY, 208 4100 STRANDHERD DR SW P3A1E2 1231231234",
    "4100 STRANDHERD DR SW",
    "P3A1E2",
    "1231231234"],

    ["UNIT 311, 15 MTN AVE CRES S K7K0J2 4161451556",
    "15 MTN AVE CRES S",
    "K7K0J2",
    "4161451556"],

    ["UNIVERSITY OF OTTAWA, DEPARTMENT OF ANESTHESIOLOGY, AND PAIN MEDICINE, 1053 CARLING AVE RD S N8N0B3 4569332223",
    "1053 CARLING AVE RD S",
    "N8N0B3",
    "4569332223"],

    ["IONA DOCTORS CLINIC, IONA PLZ, 1585 MISSISSAUGA VLY BLVD K0A0A2 8422342345",
    "1585 MISSISSAUGA VLY BLVD",
    "K0A0A2",
    "8422342345"], 

    ["STE 300 301,",
    None,
    None,
    None]]) 

    t_arr = arr.transpose()

    frame = pd.DataFrame(t_arr, ["actual", "Address_expected", "Postal_expected", "Phone_expected"])

    df = frame.transpose()

    print(type(df), " test data created")
    
    return df 

# %%