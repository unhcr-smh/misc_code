# UNHCR Python Code

- Click [here](#third-section) to get to the third section.

## misc_code -- Various Python scripts, mostly relating to data.

The [Github](https://github.com/) repo is located at:
<https://github.com/unhcr-smh/misc_code>

If you need access contact Steve Hermes:
<hermes@unhcr.org>    or    <stevehermes@gmail.com>

All the python scripts and notebooks are designed to run from the root of the repository using Python 3.12.3:

	git clone https://github.com/unhcr-smh/misc_code.git

	cd misc_code

	python -m venv .venv

	.venv\Scripts\activate

	pip install -r requirements.txt

After installing all the requirements, you must get the **config.ini** file and place it in the root directory.

- See [Config File](#config-file) to create the file.

## Directory structure

| DIR  | DESCR  |
|---|---|
|.git|  Github metadata and object database for your project|
|.venv| Python virtual environment|
|.vscode| MS VsCode -- only if you use VsCode|
|[ADAM_NOTEBOOKS](#adam_notebooks-directory)| Eyedro Greenbox scripts|
|alpg| Artifical load profile generator -- **not used**|
|Download-UNHCR-files_Save-2-PG| Adam code  -- **not used**|
|J1939DB| Canbus code -- requires SAE J1939 spec to work  -- **not used**|
|[kobo_gen_survey](#kobo_gen_survey-directory)| Adam Generator Survey scripts|
|LOR-EOR| Generator efficiency opportunity scripts|
|nanonets-ocr-sample-python|Process Generator Survey photos|
|NIGERIA_FUEL_BIOHENRY| Nigeria Fuel sensors (Galooli) scripts|
|OPEN_API| OPEN AI scripts  -- **not used**|
|SIM| Misc Simulation code WIP   -- **not used**|
|Tesseract-OCR| Optical Char Reader   -- **not used**|

### ADAM_NOTEBOOKS Directory

| DIR  | DESCR  |
|---|---|
|Meter Inventory| Original Adam Excel file  -- **not used**|
|Meter Readings| Original Adam Python Notebooks with copies and smh fixes  -- **mostly not used**|
|Verbose Meter Details| Original Adam Python Notebooks with copies and smh fixes  -- **mostly not used**|

| Files  | DESCR  |
|---|---|
|exist.py| Test script -- **not used**|
|existing-meters-smh-multi.py| Test script using parallelism WIP -- **not used**|
|[**existing-meters-smh.py**](./ADAM_NOTEBOOKS/existing-meters-smh.py)| Run every few days to get most recent data for all GBs|
|gbcommon copy.py| Backup file TODO remove -- **not used**|
|[**gbcommon.py**](./ADAM_NOTEBOOKS/gbcommon.py)| Common functions used by new-meters-smh.py and existing-meters-smh.py|
|[**new-meters-smh.py**](./ADAM_NOTEBOOKS/new-meters-smh.py)| Run once a week to check for new GBs |
|Parsing Error Log.xlsx| Error logging -- **not used**|

Two scripts in the root of this directory are run regularly to get Eyedro GreenBox data:

    2024-04-26 NOTE: The config.ini file stored at https://account.proton.me/drive points to the 2024 cloud DB. You should not have to change it till 2025

[**existing-meters-smh.py**](./ADAM_NOTEBOOKS/existing-meters-smh.py)
Run this script every few days to update all GB tables to the cloud DB. 

[**new-meters-smh.py**](./ADAM_NOTEBOOKS/new-meters-smh.py)
Run this script once a week to add all new GBs to the cloud DB.
There is a date that you should set when you run the script. Set it to one month prior to the rundate:

    utc_midnight_date = gb.dt2utc(dt(year=2024, month=4, day=1, hour=0, minute=0, second=0))

These scripts save the data in one of two Postgres cloud DBs.
Change this line to select either the 2023 or 2024 DB:

    ENGINE, schema = gb.create_db_engine(yr=2024)
    or
    ENGINE, schema = gb.create_db_engine(yr=2023)

Data for all GBs from 2023 (and earlier) are saved in this cloud DB: 

![gb 2023 db](./DOCS/gb-2023-db.png)

Data for all GBs for 2024 are saved in this cloud DB:

![gb 2024 db](./DOCS/gb-2024-db.png)

     2024-04-26 NOTE: Ideally we would store the data to one cloud DB. But the free Aiven cloud DB we use is free and has limited storage. The DB can store about 1 year of 15 minute data for 600 GBs 

The [**gbcommon.py**](./ADAM_NOTEBOOKS/gbcommon.py) script is used by the other scripts in this directory to perform common functions like:

- read the **config.ini** file
- connect to DB based on the configuration
- API calls
- Utility functions
- Data Post processing

### **kobo_gen_survey** Directory

| DIR  | DESCR  |
|---|---|
|01 Kobo Form Design| Adam original survey definitions|
|download_survey_pics| Scripts to download pics from generator survey|
|ocr| Misc Test code  -- **not used**|
|org| Backup of Adam original code TODO remove  -- **not used**|
|tech questions| Adam original  -- **not used**|

| Files  | DESCR  |
|---|---|
|[**Data Review.sql**](./kobo_gen_survey/Data%20Review.sql)| Adam Generator Survey - SQL code updated|
|[**Generator Survey MSRP Matchup.csv**](./kobo_gen_survey/Generator%20Survey%20MSRP%20Matchup.csv)| Copy of xlsx file sheet: Generator Survey MSRP Matchup|
|Generator Survey MSRP Matchup.xlsx|Extended Adam file, add notes and pic urls|
|[**Load Survey Data to Postgres for Crosscheck.ipynb**](./kobo_gen_survey/Load%20Survey%20Data%20to%20Postgres%20for%20Crosscheck.ipynb)| Main script uses config.ini for DB and local files|
|MSRP Data (Pre-Cloud ERP Migration).xlsx| Adam original genset data before ERP migration|
|[**MSRP_precloud erp.csv**](./kobo_gen_survey/MSRP_precloud%20erp.csv)| Same as above used by scripts|
|[**survey_controller_data_from_photos.csv**](./kobo_gen_survey/survey_controller_data_from_photos.csv)| Nanonets OCR from survey photos|
|UNHCR_Generator_-_CloudERP_Update_Survey_-_all_versions_-_EnglishEN_-_2024-02-08-12-32-01.xlsx| Adam original downloaded survey results |
|[**UNHCR_Generator_-_CloudERP_Update_Survey_-_all_versions_-_EnglishEN_-_2024-02-13-10-26-54.csv**](./kobo_gen_survey/UNHCR_Generator_-_CloudERP_Update_Survey_-_all_versions_-_EnglishEN_-_2024-02-13-10-26-54.csv)| Updated downloaded survey results, sheet: UNHCR Generator - CloudERP U...|
|UNHCR_Generator_-_CloudERP_Update_Survey_-_all_versions_-_EnglishEN_-_2024-02-13-10-26-54.xlsx| Updated downloaded survey results|

The results from the survey do not line up well with Pre-ERP data.
About 14 % of the survey generators match IDs in the Pre-ERP data.
only 6% of those have usable runtime data.

#### Kobo cloud DB

![kobo db](./DOCS/kobo-db.png)

| Table / View  | DESCR  |
|---|---|
|**gen_survey_results**| See [Create **gen_survey_results** Table](#create-gen_survey_results-table)|
|**generator_survey_msrp_matchup**|Created by importing [**Generator Survey MSRP Matchup.csv**](./kobo_gen_survey/Generator%20Survey%20MSRP%20Matchup.csv)|
|**msrp_precloud_erp**|Created by importing [**MSRP_precloud erp.csv**](./kobo_gen_survey/MSRP_precloud%20erp.csv)|
|**survey_controller_data_from_photos**||
|****||
|****||

#### Create **gen_survey_results** Table

The **gen_survey_results** table is created / replaced by running the first code block in the [**Load Survey Data to Postgres for Crosscheck.ipynb**](./kobo_gen_survey/Load%20Survey%20Data%20to%20Postgres%20for%20Crosscheck.ipynb) Python notebook.

#### Create **generator_survey_msrp_matchup** Table

	The notebook uses values from the config.ini file

#### Process steps

- Create the **gen_survey_results** table. See [Create Table](#create-table)
- Run the **create or replace view vw_gen_survey_cln** script in [Data Review.sql](./kobo_gen_survey/Data%20Review.sql)

Runtime data was extracted from generator controller photos using <https://nanonets.com/>
This is a free subscription so we are limited to 500 photos, and there is no team feature

	Login is using my Google account stevehermes@gmail.com

The process to extract text from photos is:

- Log into **nanonets**
- Select the **ocr** workflow
- Upload photos
- Select a photo
- Typically the text field will have data
  - If available select or enter data for the hours, asset_id, and starts fields
  - Click approval button in lower left corner

| DESCR | COUNT  |
|---|---:|
|Total Generators Pre ERP | 3448 |
|Total Survey responses	| 583 |
|Total survey to Pre ERP ID matches	| 469 |
|Total (usable) Survey controller photos | 387 |
|Total Survey controller photos ID matches | 69 |
|Total controller photos with usable runtime | 30 |



## Third section

 adasdas
 asdasd
 adas
 das
 d

### Fourth section

asdasd
asdasdasd
asdasd


## Config File

The code requires a **config.ini** file in the root directory. It contains secrets, passwords, DB credentials, and paths.

There is a working **config.ini** file at <https://account.proton.me/drive>

	You will have to change paths and potentially DB credentials for your setup

	Contact hermes@unhcr.org for access or a download link


### Config File Format

#### KOBO section for local Postgres DB and local SOURCE_FILE:

	[KOBO]
	CREATE_ENGINE = postgresql://postgres:xxpassword@localhost:5432/kobo
	TABLE_NAME = gen_survey_results
	SHEET_NAME=gen_info_survey
	HOST = localhost
	PORT = 5432
	DATABASE = kobo
	USER = postgres
	PASSWORD = xxpassword
	SOURCE_FILE = D:\OneDrive - UNHCR\Green Data Team\09 Generators\04 Generator Data Update Survey\UNHCR_Generator_-_CloudERP_Update_Survey_-_all_versions_-_EnglishEN_-_2024-02-13-10-26-54.xlsx
	URL=https://kobo.unhcr.org/#/projects/home
	PROJECT_URL=https://kobo.unhcr.org/#/forms/aCL43bcT8rcCNhvaEVRxxv/summary
	SSO_LOGIN=hermes@unhcr.org

The **[KOBO]** section is read to get the configuration info by the **kobo_gen_survey\Load Survey Data to Postgres for Crosscheck.ipynb** Python notebook. This notebook is used for the Generator Survey data and uses the Python **.venv** virtual environment. Click [here](#third-section) for details.








 
