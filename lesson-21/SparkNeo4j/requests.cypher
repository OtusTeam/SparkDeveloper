MATCH (a)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d) RETURN a,m,d;

MATCH (tom:Person {name:"Tom Hanks"})-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors),
  (coActors)-[:ACTED_IN]->(m2)<-[:ACTED_IN]-(cocoActors)
WHERE NOT (tom)-[:ACTED_IN]->()<-[:ACTED_IN]-(cocoActors)
  AND tom <> cocoActors
RETURN cocoActors.name AS Recommended, count(*) AS Strength
ORDER BY Strength DESC
LIMIT 20;

MATCH (n:Product)
RETURN n
LIMIT 10;

MATCH (n:Product)
RETURN count(n);

SHOW CONSTRAINTS;

MATCH (o:Order)
RETURN count(o);

MATCH (p:Product)-[r:CONTAINS]->(o:Order)
RETURN count(r);

MATCH (a:Person)-[:CREATED]->(o:Order)<-[c:CONTAINS]-(p:Product)
WHERE a.name IN ['Cuba Gooding Jr.', 'Tom Hanks']
RETURN a.name, o.id, o.date, p.name, c.quantityOrdered;
