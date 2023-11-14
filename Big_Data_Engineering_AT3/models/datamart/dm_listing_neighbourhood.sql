SELECT
    listing_neighbourhood,
    year_month,
    active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    superhost_rate,
    avg_review_scores,
    percentage_change_active_listing,
    percentage_change_inactive_listing,
    total_stays,
    avg_estimated_revenue
FROM (
    SELECT
        listing_neighbourhood, -- listing_neighbourhood
        TO_CHAR(scraped_date, 'YYYY-MM') AS year_month, -- year_month
        -- active_listings_rate
        ROUND((COUNT(CASE WHEN has_availability = 't' THEN 1 END) * 100.0) / COUNT(*), 2) AS active_listings_rate,
        --min_price
        MIN(CASE WHEN has_availability = 't' AND price > 0.0 THEN price ELSE 0 END) AS min_price,
        -- max_price
        MAX(CASE WHEN has_availability = 't' AND price > 0.0 THEN price ELSE 0 END) AS max_price,
        -- median_price
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' AND price > 0.0 THEN price ELSE 0 END) AS median_price,
        -- avg_price
        ROUND(AVG(CASE WHEN has_availability = 't' AND price > 0.0 THEN price ELSE 0 END), 2) AS avg_price,
        -- distinct_hosts
        COUNT(DISTINCT host_id) AS distinct_hosts,
        -- superhost_rate
        ROUND((COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) * 100.0) / COUNT(DISTINCT host_id), 2) AS superhost_rate,
        -- avg_review_scores
        ROUND(AVG(CASE WHEN has_availability = 't' THEN review_scores_rating ELSE 0 END), 2) AS avg_review_scores,
		-- percentage_change_active_listing        
        (COUNT(CASE WHEN has_availability = 't' THEN 1 END) - LAG(COUNT(CASE WHEN has_availability = 't' THEN 1 END), 1) OVER w) * 100.0 / NULLIF(LAG(COUNT(CASE WHEN has_availability = 't' THEN 1 END), 1) OVER w, 0) AS percentage_change_active_listing,
        --percentage_change_inactive_listing
        (COUNT(CASE WHEN has_availability = 'f' THEN 1 END) - LAG(COUNT(CASE WHEN has_availability = 'f' THEN 1 END), 1) OVER w) * 100.0 / NULLIF(LAG(COUNT(CASE WHEN has_availability = 'f' THEN 1 END), 1) OVER w, 0) AS percentage_change_inactive_listing,
  		--total_stays     
        SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) ELSE 0 END) AS total_stays,
        --avg_estimated_revenue
        SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) AS avg_estimated_revenue
    FROM public_warehouse."FACT_Listings"
    WHERE price > 0.0
    GROUP BY listing_neighbourhood, year_month
    WINDOW w AS (PARTITION BY listing_neighbourhood ORDER BY TO_CHAR(scraped_date, 'YYYY-MM'))
) AS subquery
ORDER BY listing_neighbourhood, year_month

