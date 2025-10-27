SELECT MIN(t.title) AS movie_title
FROM keyword AS k, movie_info AS mi, movie_keyword AS mk, title AS t
WHERE k.keyword like '%sequel%'
    AND mi.info IN ( 'PFM:35 mm','Greece:September 2007','PFM:35 mm','Drama','Germany:113','Silent' )
    AND t.production_year > 1967
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND mk.movie_id = mi.movie_id
    AND k.id = mk.keyword_id;;
