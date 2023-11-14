--Part 3. a

SELECT
    RANK() OVER (ORDER BY r.estimated_revenue_per_listing DESC) AS rank_revenue_lessontop,
    RANK() OVER (ORDER BY p.tot_p_p DESC) AS rank_population_moreontop,
    r.listing_neighbourhood,
    r.estimated_revenue_per_listing,
    p.tot_p_m AS male,
    p.tot_p_f AS female
    -- Add more demographic details as needed
FROM (
    SELECT
        listing_neighbourhood,
        ROUND(
            SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) /
            COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END),
            2
        ) AS estimated_revenue_per_listing
    FROM public_warehouse."FACT_Listings"
    GROUP BY listing_neighbourhood
) r
LEFT JOIN (
    SELECT
        lga_code_2016,
        tot_p_m,
        tot_p_f,
        tot_p_p
    FROM public_warehouse."DIM_Census_G01"
) p 
ORDER BY rank_revenue_lessontop, rank_population_moreontop DESC;








Part 3. b

SELECT
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    AVG(avg_price) AS avg_price,
    SUM(total_stays) AS total_stays,
    AVG(avg_estimated_revenue) AS estimated_revenue_per_stay
FROM (
    SELECT
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        AVG(avg_price) AS avg_price,
        SUM(total_stays) AS total_stays,
        AVG(avg_estimated_revenue) AS avg_estimated_revenue,
        ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY AVG(avg_estimated_revenue) DESC) AS rn
    FROM public_datamart.dm_property_type
    GROUP BY
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates
) AS RankedNeighbourhoods
WHERE rn = 1
GROUP BY
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates
ORDER BY estimated_revenue_per_stay DESC
LIMIT 5;


/*
 listing_neighbourhood|property_type   |room_type      |accommodates|avg_price           |total_stays|estimated_revenue_per_stay|
---------------------+----------------+---------------+------------+--------------------+-----------+--------------------------+
Sydney               |Apartment       |Entire home/apt|           2|293.2466666666666667|     131659|      8861837.000000000000|
Northern Beaches     |Entire house    |Entire home/apt|           8|842.3722222222222222|      93351|      8261050.233333333333|
Waverley             |Entire apartment|Entire home/apt|           4|243.6166666666666667|     208249|      5419354.276666666667|
Randwick             |Apartment       |Entire home/apt|           4|205.8033333333333333|      36092|      2288358.000000000000|
North Sydney         |Apartment       |Entire home/apt|           2|217.1833333333333333|      22681|      1389028.666666666667|*/

--3.c
SELECT 
    hnh.host_neighborhood_lga,
    COUNT(DISTINCT lnh.listing_neighbourhood) AS num_distinct_listings,
    COUNT(DISTINCT hnh.num_distinct_hosts) AS num_distinct_hosts,
    COUNT(DISTINCT CASE WHEN lnh.listing_neighbourhood = hnh.host_neighborhood_lga THEN lnh.listing_neighbourhood END) AS num_listings_in_same_lga,
    COALESCE(
        COUNT(DISTINCT CASE WHEN lnh.listing_neighbourhood = hnh.host_neighborhood_lga THEN lnh.listing_neighbourhood END) 
        / NULLIF(COUNT(DISTINCT lnh.listing_neighbourhood), 0), 0
    ) AS avg_same_lga
FROM public_datamart.dm_host_neighbourhood hnh
JOIN public_datamart.dm_listing_neighbourhood lnh ON hnh.host_neighborhood_lga = lnh.listing_neighbourhood
GROUP BY hnh.host_neighborhood_lga
ORDER BY avg_same_lga DESC;










3.d

SELECT DISTINCT
    dhn.host_neighborhood_lga,
    dhnh.year,
    dhnh.month,
    dhnh.num_distinct_hosts,
    dhnh.estimated_revenue,
    dhnh.estimated_revenue_per_host,
    COALESCE(nmm.annualized_median_mortgage, 0) AS annualized_median_mortgage,
    CASE
        WHEN COALESCE(nmm.annualized_median_mortgage, 0) > 0 AND dhnh.estimated_revenue_per_host >= COALESCE(nmm.annualized_median_mortgage, 0) THEN 'Yes'
        ELSE 'No'
    END AS covers_mortgage
FROM public_datamart.dm_host_neighbourhood dhn
JOIN (
    SELECT DISTINCT
        host_neighborhood_lga,
        year,
        month,
        num_distinct_hosts,
        estimated_revenue,
        estimated_revenue_per_host,
        ROW_NUMBER() OVER (PARTITION BY host_neighborhood_lga ORDER BY year, month DESC) AS row_num
    FROM public_datamart.dm_host_neighbourhood
) dhnh ON dhn.host_neighborhood_lga = dhnh.host_neighborhood_lga
LEFT JOIN (
    SELECT DISTINCT
        listing_neighbourhood,
        AVG(median_price) AS annualized_median_mortgage
    FROM public_datamart.dm_listing_neighbourhood
    WHERE year_month >= '2022-01'
    GROUP BY listing_neighbourhood
) nmm ON dhn.host_neighborhood_lga = nmm.listing_neighbourhood
WHERE dhnh.row_num = 1
ORDER BY dhn.host_neighborhood_lga, dhnh.year, dhnh.month;




3a




