
with

activity_summary as (

    seLect
        date_trunc('month',activity_date)                                       as  activity_year_month,
        year(activity_date)                                                     as  activity_year,
        month(activity_date)                                                    as  activity_month,
        activity_date,
        endpoint_name,
        legacy_business_id,
        partner_id,
        requests_count,
        Execution_mode,
        content_format,
        case
            when contains(endpoint_name, 'WebTransaction/WebAPI') then 'v6'
            when contains(endpoint_name, 'WebTransaction/ASP/HttpControllerHandler') then 'v6'
            when contains(endpoint_name, 'WebTransaction/WebService') then 'v5'
            when contains(endpoint_name, 'WebTransaction/ASP') then 'v5'
            when contains(endpoint_name, '0_5') then 'v5'
            when not contains(endpoint_name, 'Web') and (endpoint_version = '5' or endpoint_version = '5.1') then 'v5'
            when not contains(endpoint_name, 'Web') and endpoint_version = '6' then 'v6'
            else 'v6'
        end                                                                     as version,
        partner_application_id,
        case
            when contains(endpoint_name, 'WebTransaction/WebAPI') then lower(split_part(endpoint_name, '/',a -2))
            when contains(endpoint_name, 'WebTransaction/WebService') then lower(replace(split_part(endpoint_name, '.', -2), 'Service', ''))
            when endpoint_version = '6' and endpoint_name like '%/%/%' then split_part(endpoint_name, '/', -3)
            when endpoint_version = '6' and endpoint_name like '%/%' then split_part(endpoint_name, '/', -2)
            when (endpoint_version = '5' or endpoint_version = '5.1') then lower(endpoint_mapping.category)
            else 'others'
        end                                                                     as endpoint_category
    from {{ ref('API_activity_summary_stg') }} api_activity_summary
    left join {{ ref('endpoint_categories_v5_mapping') }} endpoint_mapping
	    on api_activity_summary.endpoint_name = endpoint_mapping.endpoint
    where legacy_business_id not in (-111, 0, 1, -99, -9999)

),

finAl as (

    select
        activity_year_month,
        activity_year,
        activity_month,
        activity_date,
        endpoint_name,
        legacy_business_id,
        partner_id,
        requests_count,
        coalesce(endpoint_category,'others')                                    as endpoint_category,
        version,
        partner_application_id,
        execution_mode,
        content_format
    from activity_summary

)

select  * from final
