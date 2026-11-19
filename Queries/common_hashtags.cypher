// 1. Most Popular Topics

MATCH (:Tweet)-[:TIENE_HASHTAG]->(h:Hashtag)
RETURN h.nombre AS hashtag, count(*) AS usos
ORDER BY usos DESC LIMIT 10;