
Python 3.12.3

# misc_code

Various script, mostly relating to data.

The repo is located at:
https://github.com/unhcr-smh/misc_code

If you need access contact Steve Hermes:
hermes@unhcr.org    or stevehermes@gmail.com

All the python scripts and notebooks are designed to run from the root of the repository:

	git clone https://github.com/unhcr-smh/misc_code.git

	cd misc_code

The **config.ini** in the root directory contains secrets and passwords and paths. If your using local DBs, you with have to change the DB config in **config.ini**:

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

The **[KOBO]** section is read to get the configuration info by the **kobo_gen_survey\Load Survey Data to Postgres for Crosscheck.ipynb** python notebook.

A working "config.ini" file is available at:

https://account.proton.me/drive

Contact hermes@unhcr.org for access or a download link