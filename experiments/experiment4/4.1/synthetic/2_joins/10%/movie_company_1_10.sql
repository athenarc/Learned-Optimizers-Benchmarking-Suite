SELECT mc.company_id
    FROM movie_companies mc, company_name cn, title t
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_id < 133