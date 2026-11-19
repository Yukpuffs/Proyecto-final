// 2. Most Retweeted Accounts

MATCH (:Usuario)-[r:RETWITEO_A]->(:Tweet)
RETURN r.usuario_retuiteado AS cuenta, count(*) AS retuits
ORDER BY retuits DESC LIMIT 10;