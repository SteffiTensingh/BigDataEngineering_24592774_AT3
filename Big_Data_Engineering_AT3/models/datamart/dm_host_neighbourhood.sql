select
	hd.listing_neighbourhood as listing_neighbourhood,
    la.lga_name AS host_neighborhood_lga,
    EXTRACT(YEAR FROM hd.scraped_date) AS year,
    EXTRACT(MONTH FROM hd.scraped_date) AS month,
    COUNT(DISTINCT hd.host_id) AS num_distinct_hosts,
    SUM(hd.price * (30 - hd.availability_30)) AS estimated_revenue,
    CASE
        WHEN COUNT(DISTINCT hd.host_id) > 0 THEN SUM(hd.price * (30 - hd.availability_30)) / COUNT(DISTINCT hd.host_id)
        ELSE 0
    END AS estimated_revenue_per_host
FROM
    public_warehouse."FACT_Listings" hd
LEFT JOIN public_warehouse."DIM_LGA_Suburb" la ON UPPER(hd.host_neighbourhood) = la.suburb_name
WHERE
    hd.has_availability = 't'
GROUP BY
    hd.listing_neighbourhood ,host_neighborhood_lga, year, month
ORDER BY
    host_neighborhood_lga, year, month