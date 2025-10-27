SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2011 AND ci.person_id  >  3122922 AND ci.role_id  =  10