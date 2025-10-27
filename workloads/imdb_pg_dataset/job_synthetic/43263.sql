SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  1979 AND ci.person_id  >  914696