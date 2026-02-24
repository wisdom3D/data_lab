-- Exercise 1 - Part 2: Analytics & Spatial Queries (PostGIS)

-- 1. Compute the daily average value per category over the last 30 days.
SELECT 
    DATE(timestamp) AS event_date,
    category,
    AVG(value) AS avg_value
FROM events
WHERE timestamp >= NOW() - INTERVAL '30 days'
GROUP BY event_date, category
ORDER BY event_date DESC, category;

-- 2. Compute the number of events that fall inside each predefined region over the last 30 days.
-- Assumes 'geom' in events is a POINT and 'geom' in regions is a POLYGON.
SELECT 
    r.name AS region_name,
    COUNT(e.id) AS event_count
FROM regions r
LEFT JOIN events e ON ST_Within(e.geom, r.geom)
WHERE e.timestamp >= NOW() - INTERVAL '30 days' OR e.timestamp IS NULL
GROUP BY r.id, r.name
ORDER BY event_count DESC;

-- 3. Identify the top 5 “hotspots” (small grid cells) with the highest total value in the last 7 days.
-- Using ST_SnapToGrid to group nearby points into grid cells of 0.1 x 0.1 decimal degrees.
SELECT 
    ST_AsText(ST_SnapToGrid(geom, 0.1)) AS grid_cell,
    SUM(value) AS total_value,
    COUNT(*) AS event_count
FROM events
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY grid_cell
ORDER BY total_value DESC
LIMIT 5;
