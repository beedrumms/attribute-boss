#%% IMPORT PACKAGES
import pandas as pd
import numpy as np
import pytest

import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from spark_tests_setup import MyTestingSparkSession

### 

#%% CREATE SPARK DFs 
@pytest.fixture(scope="function")
def MyTestingData(MyTestingSparkSession): 

    fake_messy_data = [("", "Aycan", "401 Smyth Rd, Children's Hospital of Eastern Ontario (CHEO)", 9043021023, "m5r 2P8", "Ontario"),
    ("NONE", "Iskhakova   ", "800 Commissioners Rd.E, PO box 5010, Station B, LHSC, Department of Medicine/Schulich,, C6-400", "023-012-1023", "T8R9J3  ", "quebec"),
    (np.nan, "  Fabian", "The Montfort Hospital, 713 Montreal Road  ", "120.091.1023", "  k0j   2t6", "Washington"),
    ("NA", "Blackmoré", "Quite Health Care, Trenton Memorial Hospital, 242 King Avenue Street e", "120291", "98470", "Alberta"),
    ("", "Pokrupà-Nahanni", "19 Hospital Road street north west, P.O. Box 664", "(504)-230-4031", "k5r-2p4", "ON"),
    ('', "", "  Suite 209, 5293 Highway 7 East", "1-(504)-213-0421", "M 4R 2P7", "ontario"),
    ("na", "Schnarr", "HHS St Peter's Hospital, 88 Maplewood Ave", "1905213042", " k4r2t5", "QC"),
    ("none", "Peticca ", "  LMC Ottawa Diabetes & Endocrinology, 208-4100 Strandherd Drive South West", "1416-123-3041", "m3k4k9", "British Columbia"),
    ("None", "NA", "Unit 311 , 15 Mountain Avenue South", "2301233921", "M5R2P8", "ontario"),
    ("Na", "Abdallah”", "University of Ottawa, Department of Anesthesiology, and Pain Medicine, 1053 Carling Ave road s", "" , "k0r1p9", ""),
    (np.nan, " Papnéjà", "Iona Doctors     Clinic   , Iona Plaza, 1585 Mississauga Valley Boulevard", "8123", "M6T   2P4", "ONT"),
    ("YAY", "Reyes¢", "Ste 300-301, 40 Holly Street", "NONE", "M4r-208", "New York")]

    fake_expected_data = [(None, "AYCAN", "401 SMYTH RD, CHILDRENS HOSPITAL OF EASTERN ONTARIO (CHEO)",
    "401 SMYTH RD, CHILDRENS HOSPITAL OF EASTERN ONTARIO (CHEO)",
    "401 SMYTH RD" 
    "9043021023", "M5R2P8", "ONTARIO"),

    (None, "ISKHAKOVA", "800 COMMISSIONERS RD E, PO BOX 5010, STATION B, LHSC, DEPARTMENT OF MEDICINE/SCHULICH, C6-400", 
    "800 COMMISSIONERS RD E, PO BOX 5010, STATION B, LHSC, DEPARTMENT OF MEDICINE/SCHULICH, C6-400",
    "800 COMMISSIONERS RD E",
    "0230121023", "T8R9J3", "QC"),

    (None, "FABIAN", "THE MONTFORT HOSPITAL, 713 MONTREAL ROAD",
    "THE MONTFORT HOSPITAL, 713 MONTREAL RD",
    "713 MONTREAL RD",
    "1200911023", "K0J2T6", "WA"),

    (None, "BLACKMORE", "QUITE HEALTH CARE, TRENTON MEMORIAL HOSPITAL, 242 KING AVENUE STREET E",
    "QUITE HEALTH CARE, TRENTON MEMORIAL HOSPITAL, 242 KING AVE ST E",
    "242 KING AVE ST E",
    "120291", "98470", "AB"),

    (None, "POKRUPA-NAHANNI", "19 HOSPITAL ROAD, PO BOX 664",
    "19 HOSPITAL RD ST NW, PO BOX 664",
    "19 HOSPITAL RD ST NW",
    "5042304031", "K5R2P4", "ON"),

    (None, "SCHNARR", "SUITE 209, 5293 HIGHWAY 7 EAST",
    "SUITE 209, 5293 HWY 7 E",
    "5293 HWY 7 E",
    "15042130421", "M4R2P7", "ON"), 

    (None, "SCHNARR", "HHS ST PETERS HOSPITAL, 88 MAPLEWOOD AVE",
    "HHS ST PETERS HOSPITAL, 88 MAPLEWOOD AVE",
    "88 MAPLEWOOD AVE",
    "1905213042", "K4R2T5", "QC"), 

    (None, "PETICCA", "LMC OTTAWA DIABETES & ENDOCRINOLOGY, 208-4100 STRANDHERD DRIVE SOUTH WEST",
    "LMC OTTAWA DIABETES & ENDOCRINOLOGY, 208-4100 STRANDHERD DR SW",
    "4100 STRANDHERD DR SW",
    "14161233041", "M3K4K9", "BC"),

    (None, "UNIT 311, 15 MOUNTAIN AVENUE SOUTH",
    "UNIT 311, 15 MOUNTAIN AVE S",
    "15 MOUNTAIN AVE S",
    "2301233921", "M5R2P8", "ON"),

    (None, "ABDALLAH", "UNIVERSITY OF OTTAWA, DEPARTMENT OF ANESTHESIOLOGY, AND PAIN MEDICINE, 1053 CARLING AVE ROAD S",
    "UNIVERSITY OF OTTAWA, DEPARTMENT OF ANESTHESIOLOGY, AND PAIN MEDICINE, 1053 CARLING AVE RD",
    "1053 CARLING AVE RD S",
    None, "K0R1P9", None),

    (None, "PAPNEJA", "IONA DOCTORS CLINIC, IONA PLAZA, 1585 MISSISSAUGA VALLEY BOULEVARD",
    "IONA DOCTORS CLINIC, IONA PLAZA, 1585 MISSISSAUGA VLY BLVD",
    "1585 MISSISSAUGA VLY BLVD",
    "8123", "M6T2P4", "ON"), 

    ("YAY", "REYES", "STE 300-301, 40 HOLLY STREET",
    "STE 300-301, 40 HOLLY ST",
    "40 HOLLY ST",
    None, "M4R208", "New York")] 

    messy_df = spark.createDataFrame(fake_messy_data, ["Nulls", "Name" "Address", "Phone", "Postal", "Prov"])
    assert_df = spark.createDataFrame(fake_expected_data, ["Nulls", "Name", "Address_Features", "Address_Standardizer", "Address_Extractor", "Phone", "Postal", "Prov"])
    comparison_df = spark.createDataFrame([("", ""), ("", "")], ["actual", "expected"])

    yield messy_df, assert_df, comparison_df


    messy_df.unpersist()
    assert_df.unpersist()
    comparison_df.unpersist()
    
# %%