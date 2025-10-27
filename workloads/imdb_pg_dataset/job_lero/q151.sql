SELECT MIN(t.title) AS movie_title
FROM keyword AS k, movie_info AS mi, movie_keyword AS mk, title AS t
WHERE k.keyword like '%sequel%'
    AND mi.info IN ( 'USA:28 April 2008','Germany:o.Al.','English','Serbo-Croatian','5' )
    AND t.production_year > 1986
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND mk.movie_id = mi.movie_id
    AND k.id = mk.keyword_id;;
