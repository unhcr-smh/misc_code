/*****************************************************************
Generator Survey - MSRP Comparison
Created by: A. Fifield
Last Modified: 2024-02-13

	1.1 Create view of survey data (vw_gen_survey_cln)
	1.2 Compare survey results against MSRP data
	
*****************************************************************/

-- Items to Check:

-- Duplicated Asset IDs
-- Clearly Incorrect Asset IDs
-- Asset ID found but data looks incorrect
-- Asset ID match and data reasonably matches (KVA rating + Make)
-- Asset ID match and data matches (KVA rating + Make)


/*****************************************************************
1.1 Create view of survey data (vw_gen_survey_cln)
*****************************************************************/

drop view vw_gen_survey_cln;

create or replace view vw_gen_survey_cln
as
select 
	"index"
	, "asset_id"
	, "asset_id_num"
	, "kva_rating"
	, case when "gen_make" <> 'Other' then "gen_make" 
			else "gen_make_other" 
				end as "gen_make"
	, case when "gen_manufacturer" like 'Other%' then "gen_manufacturer_other"
			when "gen_manufacturer" is null then "gen_manufacturer_other"
			else "gen_manufacturer" 
				end as "gen_manufacturer"
	, case when "gen_model" is not null then "gen_model"
			else "gen_model_other" 
				end as "gen_model"
	, case when ("asset_id_num" < 0 
					or length("asset_id_num"::varchar) not in (6,7)
					or "asset_id_num" is null) then true
			else false 
				end as "asset_id_issue"
	, "dupe_asset_id"
from gen_survey_results
;

/*****************************************************************
1.2 Compare survey results against MSRP data
*****************************************************************/

select
	vw_gen_survey_cln."index" as "survey_index"
	, vw_gen_survey_cln."asset_id" as "survey_asset_id"
	, survey_msrp_match."MSRP_ASSET_ID" as "msrp_asset_id_match"
	, vw_gen_survey_cln."asset_id_issue" as "survey_asset_id_issue"
	, vw_gen_survey_cln."dupe_asset_id" as "survey_asset_id_duplicate"
	, vw_gen_survey_cln."gen_make" as "survey_gen_make"
	, vw_gen_survey_cln."gen_manufacturer" as "survey_gen_manufacturer"
	, vw_gen_survey_cln."gen_model" as "survey_gen_model"
	, vw_gen_survey_cln."kva_rating" as "survey_kva_rating"
	, survey_msrp_match."ASSET_DESCRIPTION_" as "msrp_asset_desc_one"
	, survey_msrp_match."ASSET_DESCRIPTION" as "msrp_asset_desc_two"
	, survey_msrp_match."MANUFACTURER" as "msrp_manufacturer"
	, survey_msrp_match."MODEL_" as "msrp_model"
	, survey_msrp_match."ASSET_KEY_SEGMENT1" as "msrp_custodian"
from vw_gen_survey_cln
left join (select 
				survey."index"
				, msrp."MSRP_ASSET_ID"
				, msrp."ASSET_DESCRIPTION"
				, msrp."TAG_NUMBER"
				, msrp."MANUFACTURER"
				, msrp."MODEL_"
				, msrp."ASSET_KEY_SEGMENT1" 
				, msrp."ASSET_KEY_SEGMENT2"
				, msrp."ASSET_DESCRIPTION_"
			from vw_gen_survey_cln as survey
			left join gen_inventory_pre_cloud_erp as msrp
			on survey."asset_id_num" = msrp."MSRP_ASSET_ID"
			where survey."asset_id_issue" = false
			and survey."dupe_asset_id" = false) as survey_msrp_match
on vw_gen_survey_cln."index" = survey_msrp_match."index"
;


