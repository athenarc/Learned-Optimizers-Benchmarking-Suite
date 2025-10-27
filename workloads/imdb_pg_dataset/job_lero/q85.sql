SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info_idx AS mi_idx, title AS t
WHERE ct.kind = 'distributors'
    AND it.info = 'rating'
    AND mc.note not like '%med%'
    AND t.production_year between 1965
    AND 1994
    AND ct.id = mc.company_type_id
    AND t.id = mc.movie_id
    AND t.id = mi_idx.movie_id
    AND mc.movie_id = mi_idx.movie_id
    AND it.id = mi_idx.info_type_id;;
