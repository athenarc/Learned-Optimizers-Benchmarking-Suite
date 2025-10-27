SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE ct.kind = 'production companies'
    AND mc.note like '%%'
    AND mc.note like '%%'
    AND mi.info IN ( '30','UK:15','Color','RAT:1.33 : 1','Black
    AND White','Color','Black
    AND White','USA','Short' )
    AND t.production_year > 2002
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND mc.movie_id = mi.movie_id
    AND ct.id = mc.company_type_id
    AND it.id = mi.info_type_id;;
