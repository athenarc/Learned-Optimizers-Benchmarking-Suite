SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  >  1967 AND ci.person_id  =  1848700 AND mk.keyword_id  <  25711