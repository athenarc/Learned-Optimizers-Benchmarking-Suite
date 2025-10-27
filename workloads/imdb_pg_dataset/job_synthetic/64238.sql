SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  =  2009 AND ci.person_id  <  3520439 AND mk.keyword_id  <  11565