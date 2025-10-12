SELECT mc.company_id
    FROM movie_companies mc, company_name cn
    WHERE mc.company_id = cn.id AND mc.company_id < 133