SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  >  2012 AND ci.person_id  >  2628858 AND ci.role_id  =  3 AND mk.keyword_id  >  335