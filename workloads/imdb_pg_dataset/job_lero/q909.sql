SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE ct.kind = 'production companies'
    AND mc.note like '%a%'
    AND mc.note like '%a%'
    AND mi.info IN ( 'Catalan','English','USA:60','Black
    AND White','USA:18 October 2012','30' )
    AND t.production_year > 2012
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND mc.movie_id = mi.movie_id
    AND ct.id = mc.company_type_id
    AND it.id = mi.info_type_id;;
