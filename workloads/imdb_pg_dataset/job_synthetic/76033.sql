SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND mc.company_id  >  79698 AND ci.person_id  >  2975736 AND ci.role_id  =  10