# UNHCR Python Code

- Click [here](#third-section) to get to the third section.

## misc_code -- Various Python scripts, mostly relating to data.

The [Github](https://github.com/) repo is located at:
https://github.com/unhcr-smh/misc_code

If you need access contact Steve Hermes:
hermes@unhcr.org    or    stevehermes@gmail.com

All the python scripts and notebooks are designed to run from the root of the repository using Python 3.12.3:

	git clone https://github.com/unhcr-smh/misc_code.git

	cd misc_code

	python -m venv .venv

	.venv\Scripts\activate

	pip install -r requirements.txt

After installing all the requirements, you must get the **config.ini** file and place it in the root directory.

- See [Config File](#config-file) to create the file.

### Directory structure

| DIR  | DESCR  |
|---|---|
|.git|  Github metadata and object database for your project|
|.venv| Python virtual environment|
|.vscode| MS VsCode -- only if you use VsCode|
|[ADAM_NOTEBOOKS](#adam_notebooks)| Eyedro Greenbox scripts|
|alpg| Artifical load profile generator -- **not used**|
|Download-UNHCR-files_Save-2-PG| Adam code  -- **not used**|
|J1939DB| Canbus code -- requires SAE J1939 spec to work  -- **not used**|
|kobo_gen_survey| Adam Generator Survey scripts|
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

I mostly use the files in the root of this directory

| Files  | DESCR  |
|---|---|
|exist.py| Test script -- **not used**|
|existing-meters-smh-multi.py| Test script using parallelism WIP -- **not used**|
|existing-meters-smh.py| Run every few days to get most recent data for all GBs|
|gbcommon copy.py| Backup file TODO remove -- **not used**|
|gbcommon.py| Common functions used by new-meters-smh.py and existing-meters-smh.py|
|new-meters-smh.py| Run once a week to check for new GBs |
|Parsing Error Log.xlsx| Error logging -- **not used**|

These scripts are run regularly to get Eyedro GreenBox data:

**existing-meters-smh.py**
	Saves data in one of two Postgres DBs. 

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

There is a working **config.ini** file at https://account.proton.me/drive

	You will have to change paths and potentially DB credentials for your setup

	Contact hermes@unhcr.org for access or a download link
	

### Config File Format

Example KOBO section for local Postgres DB and local SOURC_FILE:
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

The **[KOBO]** section is read to get the configuration info by the **kobo_gen_survey\Load Survey Data to Postgres for Crosscheck.ipynb** python notebook. This notebook uses the Python **.venv** virtual environment.

There is a cloud based Postgres DB, the **config.ini** file stored in  is set to use it:


A working "config.ini" file is available at:![kobo db](./DOCS/kobo-db.png)




 
