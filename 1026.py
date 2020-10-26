

import redis
import psycopg2 as pgsql
from opcua import Client,ua
import logging
import time
from threading import Thread
import datetime

R1 = redis.Redis(host='127.0.0.1', port=6379, db=0, encoding='utf-8')
URL = "opc.tcp://192.168.0.21:4862"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s-%(filename)s[line:%(lineno)d]-%(levelname)s:%(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

def use_time(func):
    def wrapper():
        start = time.time()
        func()
        end = time.time()
        print(f"本次数据读取与写入数据库消耗的时间{end-start}")
    return wrapper



class ProductClass:
    def __init__(self):
        self.url = URL
        self.ua_c = None
        self.root_node = None
        self.s7_root_node = None
        self.mod_root_node = None
        self.redis_c = None
        self.nodes_list = None

    # ua client
    def ua_client(self):
        self.ua_c = Client(url=self.url)
        try:
            self.ua_c.connect()
            self.root_node = self.ua_c.get_root_node().get_child(["0:Objects", "1:WinCC", "1:@LOCALMACHINE::"])
            self.s7_root_node = self.root_node.get_child(["1:SIMATIC S7-1200, S7-1500 Channel"])
            self.mod_root_node = self.root_node.get_child(["1:Modbus TCPIP"])
            self.get_nodes([self.s7_root_node, self.mod_root_node])
        except Exception as e:
            print(e)
            logger.warning(e)
        finally:
            self.ua_c.disconnect()

    # 获取节点
    def get_nodes(self, nodes=[]):
        #index = 1
        while len(nodes):
            node = nodes.pop()
            c_name = node.get_node_class().name
            b_name = node.get_browse_name().Name
            if c_name == "Variable":
                if ua.AccessLevel.CurrentRead in node.get_access_level():
                    # yield node
                    # R1.set(index, str(node))
                    R1.sadd('nodes',str(node))
                    # index += 1
                    # logger.debug(f"节点遍历{round(index / 4187 * 100, 1)}%")
            elif c_name == "Object" and b_name not in ["WNTS01", "WNTS02", "WNTS03"]:
                for i in node.get_children():
                    nodes.append(i)
            else:
                continue


def insert_obj(ostr): 
    try:
        db = pgsql.connect(host='49.233.21.143', port=5423, user='postgres', password='qwe123', database='water_treatment')
        cur = db.cursor()
        sql = 'insert into ods.km6_obj(oname,otime,ovalue,oquality) values %s ' %(ostr)
        cur.execute(sql)
        db.commit()
        cur.close()
        db.close()
    except Exception:
        logger.warning("数据插入失败", Exception)


@use_time
def get_data():
    c = Client(url=URL)
    try:
        c.connect()
        nodeid_list = [c.get_node(one.decode()).nodeid for one in R1.smembers('nodes')]
        res = c.uaclient.get_attributes(nodeid_list,ua.AttributeIds.Value)
        res_ = list(zip(nodeid_list,res))
        res__ = [(i[0].Identifier,datetime.datetime.now().strftime(r"%Y-%m-%d %H:%M:%S"), i[1].Value.Value,i[1].StatusCode.value) for i in res_]   
        insert_obj(str(res__).replace('[','').replace(']',''))  
    except Exception as e:
        print(e)
    finally:
        c.disconnect()




if __name__ == "__main__":
    p = ProductClass()
    p.ua_client()
    print(f"节点遍历完成,总共遍历出{R1.smembers('nodes')}个节点")
    # print(R1.scard('nodes'))
    # redis_nodes = R1.smembers('nodes')
    # redis_nodes_list = [onenode.decode() for onenode in redis_nodes]
    # print(len(redis_nodes_list))
    #test()
    while True:
        time.sleep(1.5)
        try:
            get_data()
        except Exception as e:
            print(e)
            continue









