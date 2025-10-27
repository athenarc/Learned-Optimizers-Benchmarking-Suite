SELECT MIN(t.title) AS movie_title
FROM keyword AS k, movie_info AS mi, movie_keyword AS mk, title AS t
WHERE k.keyword like '%sequel%'
    AND mi.info IN ( '-','Drama','Stereo','60','Roop Tara Studios, Mumbai, Maharashtra, India','PFM:35 mm','PFM:35 mm','Spain:24 January 2008','OFM:35 mm','Austria' )
    AND t.production_year > 1963
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND mk.movie_id = mi.movie_id
    AND k.id = mk.keyword_id;;
