SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE ct.kind = 'production companies'
    AND mc.note like '% (T%'
    AND mc.note like '% (T%'
    AND mi.info IN ( 'Color','Belgium','Philippines','Canada','Short','Argentina:30','Black
    AND White','USA:22 May 2007','RAT:1.20 : 1','Mono' )
    AND t.production_year > 1974
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND mc.movie_id = mi.movie_id
    AND ct.id = mc.company_type_id
    AND it.id = mi.info_type_id;;
