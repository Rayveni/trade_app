import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values

class pg_wrapper:
    __slots__= ['conn_url']
    
    def __init__(self,conn_url:str):
        self.conn_url = conn_url
        
    def base_operation(self,f,**kwargs:dict)->None:
        with  psycopg2.connect(self.conn_url) as conn:
            with conn.cursor() as cur:
                f(cur,**kwargs)
                n_affected=cur.rowcount
        return n_affected
                
    def _execute(self,query:str)->None:
        f=lambda cursor,query:cursor.execute(query)
        return self.base_operation(f,query=query)
        
    def insert_many(self,table:str,columns:str,values_list:list):
        query=f"insert into {table}({','.join(columns)}) values %s".replace('group','"group"')
        f=lambda cursor,values:execute_values(cursor, query, values)
        return self.base_operation(f,values=values_list)

    def truncate(self,table:str):
        query=f"truncate {table}"
        return self._execute(query)
    
    def fetch_all(self,query:str,return_type:str='json')->tuple:
        try:
            with  psycopg2.connect(self.conn_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    headers=tuple([column.name for column in cur.description])
                    data=cur.fetchall()
                    res=(True,self.__convert_query_result(headers,data,return_type))
        except (Exception, Error) as error:
            res=(False,str(error))
        return res
    
    def __convert_query_result(self, headers,data,convert_type:str='json')->list:
        if convert_type=='json':
            res=[dict(zip(headers,row)) for row in data]
        else:
          res=[headers] +data
        return res
        
        