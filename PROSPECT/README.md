Prospect dashboards and Postman info


#### Postman Collections\PROSPECT REST API basics- CRUD.postman_collection-v1.json

## Demo Unifier Building Locations

### link to:

    https://app.prospect.energy/analytics/fdstxhwxopkhsc

### query

    select custom->>'country' country, building_name, 
    latitude, longitude, status, 
    split_part(building_name, ' ', 2) grid_name,
    custom->>'serial_number' serial_number,
    custom->>'type' type
    from custom_286_unifier_custom 
    where custom->>'country' IN ($country)
    and custom->>'type' IN ($type)
    and status IN ( $status  ) 
    and split_part(building_name, ' ', 2) in ($offices)

### var-offices
    SELECT split_part(building_name, ' ', 2) office FROM custom_286_unifier_custom
    where custom->>'country' IN ('$country') 

### gb (serial_number)
    select serial_number from data_meters
    where grid_name in ('$offices') and serial_number like '%-%'

### Postman 

    Post Unifier Custom Building Locations

    {
        "external_id": "Hungary_HUNBUDOHQ01_HQ Budapest 01",
        "building_code": "HUNBUDOHQ01",
        "name": "HQ Budapest 01",
        "latitude": 47.52011,
        "longitude": 19.055035,
        "status": "Open",
        "country": "Hungary",
        "type": "office"
    }


## Demo Green Boxes by Site

### link to:

    https://prodata.prospect.energy/d/fdsnmcs6jym80d?var-offices=Obo&var-country=Central%20African%20Republic

### query

    WITH pre AS (
    SELECT distinct time_bucket_gapfill('1 $range', metered_at) AS time,
        m.serial_number  id, 
        -- (m.primary_use::json)->>label id,
        -- MAX(energy_lifetime_wh) - MIN(energy_lifetime_wh) AS energy
        $aggr(energy_interval_wh) AS energy
    FROM data_meters_ts mts
    LEFT JOIN data_meters m on mts.serial_number = m.serial_number
    WHERE metered_at between '$year-01-01' and '$year=12=31 23:59'
    AND m.organization_id = 89 
    and mts.energy_interval_wh != 0
    and char_length(m.primary_use) > 10 
    and grid_name in ('$offices')
    GROUP BY 1,2
    ORDER BY 1
    )
    SELECT time , id,
    energy AS "wh $aggr $range"
    FROM pre
    where id in ($gb)

### Postman

    Post Unifier Custom GBs

    {
        "external_id": "Hungary_HUNBUDOHQ01_HQ Budapest 01",
        "building_code": "HUNBUDOHQ01",
        "record_updated": "07/08/2024",
        "status": "Active",
        "uom": "KWH",
        "avg_uom_daily": 327,
        "model": "Eyedro-EMW",
        "cumulative_reading": "189,244.00",
        "sensor_type": "ESCLV-31-400A",
        "install_date": "12/07/2022",
        "serial_number": "009-80B22",
        "manufacturer": "Eyedro",
        "model__1": "EMW.SMOD.3LV",
        "activation_date": "12/07/2022",
        "country": "Hungary",
        "building_name": "HQ Budapest 01",
        "type": "gb"
    }

## Demo Green Box Data


### link to:

     https://prodata.prospect.energy/d/adtk54bhxotfkd?var-offices=$offices&var-country=$country

### query

    SELECT metered_at, energy_interval_wh FROM data_meters_ts
    where serial_number = '$gb'

### Postman

    Post Meter Readings

    {
		"manufacturer" : "EYEDRO",
		"serial_number" : "009-80AA1",
		"metered_at" : "2023-01-01T00:00:00+00:00",
		"phase" : 1,
		"voltage_v" : 220,
		"energy_interval_wh" : 0E-20,
		"power_factor" : 0.8,
		"frequency_hz" : 50,
		"interval_seconds" : 900
	}

## Demo Site Fuel

### link to:

    https://prodata.prospect.energy/d/edtmf6apha2gwf?var-id=$offices_upper

### query

    SELECT external_id, custom FROM custom_279_fuel_data 
    where external_id like concat($id,'_OFF%')
    order by external_id desc

### Postman

    Post data Fuel Pwr 2023 Abuja

	{
		"external_id" : "ABUJA_OFFICE_DG1_and_DG2_1700780400_OFFICE-00980785",
		"start" : "2023-11-24 00:00:00",
		"end" : "2023-11-24 00:15:00",
		"tank1" : 316.670,
		"tank2" : 373.600,
		"delta1" : 0.000,
		"delta2" : 0.000,
		"gb1" : 2.756
    }
