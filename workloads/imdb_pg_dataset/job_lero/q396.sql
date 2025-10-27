SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE ct.kind = 'production companies'
    AND mc.note like '%%'
    AND mc.note like '%%'
    AND mi.info IN ( 'USA:10 February 2012','Documentary','Short','Chicago, Illinois, USA','Crime','UK','PFM:35 mm' )
    AND t.production_year > 1983
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND mc.movie_id = mi.movie_id
    AND ct.id = mc.company_type_id
    AND it.id = mi.info_type_id;;
