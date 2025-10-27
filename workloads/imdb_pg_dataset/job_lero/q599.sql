SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE ct.kind = 'production companies'
    AND mc.note like '%nga%'
    AND mc.note like '%nga%'
    AND mi.info IN ( '45','Australia:3 December 2009','USA:21 February 2009','West Germany','Argentina','Color','RAT:1.33 : 1','PFM:35 mm','Short','Drama' )
    AND t.production_year > 2000
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND mc.movie_id = mi.movie_id
    AND ct.id = mc.company_type_id
    AND it.id = mi.info_type_id;;
