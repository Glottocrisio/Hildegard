class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


conn = Neo4jConnection(uri="bolt://52.87.205.91:7687", 
                       user="neo4j",              
                       pwd="A_BxjxPL72PiFRF")

# per evitare duplicati nella visualizzazione del grafo
conn.query('CREATE CONSTRAINT papers IF NOT EXISTS ON (p:Paper)     ASSERT p.id IS UNIQUE')

def add_categories(categories):
    # Adds category nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (c:Category {category: row.category})
            RETURN count(*) as total
            '''
    return conn.query(query, parameters = {'rows':categories.to_dict('records')})


def add_people(rows, batch_size=10000):
    # Adds author nodes to the Neo4j graph as a batch job.
    query = '''
            UNWIND $rows AS row
            MERGE (:Author {name: row.author})
            RETURN count(*) as total
            '''
    return insert_data(query, rows, batch_size)


#def insert_data(query, rows, batch_size = 10000):
#    # Function to handle the updating the Neo4j database in batch mode.
    
#    total = 0
#    batch = 0
#    start = time.time()
#    result = None
    
#    while batch * batch_size < len(rows):

#        res = conn.query(query, 
#                         parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
#        total += res[0]['total']
#        batch += 1
#        result = {"total":total, 
#                  "batches":batch, 
#                  "time":time.time()-start}
#        print(result)
        
#    return result