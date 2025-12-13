from  redis import from_url
import json

class redis_steams:
    __slots__= ['conn']
    
    def __init__(self,redis_url:str):
        self.conn = from_url(redis_url)
        """
        self.topic,self.consumer_group=topic,consumer_group
        
            key: The key name of the stream.
            group: The name given to the consumer group.
            <id | $>: The ID of the last-delivered message. Use $ to start from the new messages.
            MKSTREAM (optional): Automatically create the stream if it does not exist.
         
        if consumer_group and topic:
            try:
             self.conn.xgroup_create( self.topic, self.consumer_group, id='0', mkstream=True)
            except:
                pass
        """
    def info(self):
        return self.conn.info()
    
    def __encode_message(self,message,header_dict)->dict:
        return {'message':json.dumps(message),'header':json.dumps(header_dict)}
    
    def __decode_message(self,messages_list:list)->list:
        res=[]
        topic=messages_list[0][0]
        for _message in messages_list[0][1]:
            res.append({'topic':topic,'message_id':_message[0],'message':json.loads(_message[1][b'message']),'header':json.loads(_message[1][b'header']) 
            })
        return res    
    
    def create_consumer_group(self,topic:str,consumer_group:str)->dict:
        res={}
        try:
            self.conn.xgroup_create( topic, consumer_group, id='0', mkstream=True)
            res={'status':True,'error':None}
        except Exception as e:
            res={'status':False,'error':str(e)} 
        return res      
        
    def publish(self,topic:str,message_dict:dict,header_dict:dict={'type':'default'})->bytes:
        return self.conn.xadd(topic, self.__encode_message(message_dict,header_dict)) 

    def bulk_publish(self,topic:str,message_list:list)->list:
        pipe = self.conn.pipeline()
        for _message in message_list:
            pipe.xadd(topic, self.__encode_message(_message)) 
        return pipe.execute()
    
    def delete_all(self):
        "Delete all keys in all databases on the current host"
        return self.conn.execute_command('FLUSHALL')
    
    def topic_info(self,topic:str)->dict:
        return self.conn.xinfo_stream(topic)

    def consumer_group_info(self,topic:str)->bool:
        return self.conn.xinfo_groups(topic)

    def consume(self,topic:str,consumer_group:str,count:int=1,consumer:str='default_consumer')->list:
        messages_list=self.conn.xreadgroup(consumer_group, consumer, {topic: '>'}, count=count)
        if len(messages_list)<1:
            return None
        else:
            return  self.__decode_message(messages_list)

    def commit(self,topic:str,consumer_group:str,message_id:str)->list:
        return  self.conn.xack(topic, consumer_group, message_id)    

    def get_uncommited_messages(self,topic:str,consumer_group:str,count:int=10)->list:
        return  self.conn.xpending_range(name=topic, groupname=consumer_group, min="-", max="+",count=count)

    def clear_topic(self,topic:str)->list:
        return  self.conn.xtrim(topic, 0)


class redis_dict:
    __slots__= ['conn','app_name']
    
    def __init__(self,redis_url:str,app_name:str=None):
        self.conn = from_url(redis_url)
        self.app_name=app_name
        """
        self.topic,self.consumer_group=topic,consumer_group
        
            key: The key name of the stream.
            group: The name given to the consumer group.
            <id | $>: The ID of the last-delivered message. Use $ to start from the new messages.
            MKSTREAM (optional): Automatically create the stream if it does not exist.
         
        if consumer_group and topic:
            try:
             self.conn.xgroup_create( self.topic, self.consumer_group, id='0', mkstream=True)
            except:
                pass
        """
    def convert_key(self,key:str):
        if self.app_name is not None:
            key=f'{self.app_name}_{key}'
        return key
        
    def set_single_value(self,key,value):
        return self.conn.set(self.convert_key(key), value)
    
    def get_single_value(self,key):
        return self.conn.get(self.convert_key(key))

    def set_dict(self,dict_name:str,mapping:dict):
        return self.conn.hset(self.convert_key(dict_name), mapping=mapping)

    def get_dict(self,dict_name:str,key=None):
        if key is None:
            return self.conn.hgetall(self.convert_key(dict_name))
        else:
            return self.conn.hget(self.convert_key(dict_name),key)

    def dict_len(self,dict_name:str)->int:
        return self.conn.hlen(self.convert_key(dict_name))
    
    def dict_del_key(self,dict_name:str,key)->int:
        return self.conn.hdel(self.convert_key(dict_name),key)
    
    def update_dict_value(self,dict_name:str,key,new_value):
        pipe = self.conn.pipeline()
        _dict=self.convert_key(dict_name)
        pipe.hdel(_dict,key)
        pipe.hset(_dict,key=key,value=new_value)
        return pipe.execute()
    
    def dict_add_key(self,dict_name:str,key,value)->int:
        return self.conn.hset(self.convert_key(dict_name),key=key,value=value)