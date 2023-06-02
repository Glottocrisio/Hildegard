
query_string = '''
MATCH (c:Category) 
RETURN c.category_name, SIZE(()-[:IN_CATEGORY]->(c)) AS inDegree 
ORDER BY inDegree DESC LIMIT 20
'''
top_cat_df = pd.DataFrame([dict(_) for _ in conn.query(query_string)])
top_cat_df.head(20)


#allshortest paths example

#WITH ["Keanu Reeves", "Gene Hackman", "Clint Eastwood"] AS names
#UNWIND names AS nn
#  MATCH (n {name: nn})
#  WITH collect(n) AS nds

#UNWIND nds AS n1
#  UNWIND nds AS n2
#  WITH nds, n1, n2 WHERE id(n1) > id(n2)
#    MATCH path = allShortestPaths((n1)-[*]-(n2))
#    WITH nds, path WHERE ALL(n IN nds WHERE n IN nodes(path))
#RETURN path ORDER BY length(path) ASC