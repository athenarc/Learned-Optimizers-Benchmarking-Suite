SELECT MIN(t.title) AS movie_title
FROM keyword AS k, movie_info AS mi, movie_keyword AS mk, title AS t
WHERE k.keyword like '%sequel%'
    AND mi.info IN ( 'USA','USA','PFM:35 mm','Yugoslavia','English','RAT:1.33 : 1','Color','2','OFM:35 mm','English' )
    AND t.production_year > 2005
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND mk.movie_id = mi.movie_id
    AND k.id = mk.keyword_id;;
