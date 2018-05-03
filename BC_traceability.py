#!/usr/bin/python
# -*- coding: utf-8 -*-

# Created by David Teng on 18-4-29

import pymongo
import traceback
import sys


MONGODB_CONFIG = {
    'host': '127.0.0.1',
    'port': 27017,
    'db_name': 'envdata_db',
    'username': None,
    'password': None
}

class mongops():
    """
        提供mongo数据库的操作
    """
    def __init__(self):
        # connect db
        try:
            self.conn = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
            self.db = self.conn[MONGODB_CONFIG['db_name']]  # connect db
            self.username = MONGODB_CONFIG['username']
            self.password = MONGODB_CONFIG['password']
            if self.username and self.password:
                self.connected = self.db.authenticate(self.username, self.password)
            else:
                self.connected = True
        except Exception:
            print traceback.format_exc()
            print 'Connect Statics Database Fail.'
            sys.exit(1)

    def getsalersInfo(self):
        """

        :return:
        """


    def getsalepointid(self, pname):
        res = self.db["sub_company_salepoint"].find_one({'name': pname})
        return int(res["_id"]) if res else None
        # for k in res:
        #     return k
        pass

    def getsalepointname(self, pid):
        res = self.db["sub_company_salepoint"].find_one({'_id': str(pid)})
        return res["name"] if res else None
        # for k in res:
        #     return k

class customqueue():
    """
        构建消费者队列 
    """


class ordersqueue():
    """
        为所有类型商品分别构建库存队列
    """
    def __init__(self):
        self.dbop = mongops()
        pass


    def buildordersQ(self, salepointname):
        """ 建立某一分公司所有产品库存信息 """
        orders_dic = {}
        salepointid = self.dbop.getsalepointid(salepointname)
        orders_res = self.dbop.db["orders_collection"].find({"ordered_by": salepointid}).sort("ordered_at", pymongo.ASCENDING)
        # print orders_res
        for k in orders_res:
            if k['name'] not in orders_dic.keys():
                orders_dic[k['name']] = [k,]
            else:
                orders_dic[k['name']].append(k)
        return orders_dic
        pass



def buildqueryenv():
    """
    建立查询环境，
    查询流程：根据给定的
    :return:
    """

def getsaler(name, sold_at, quantity, prodQ):
    """
        根据销售信息找到对应的产品供应商和产品批次
    :param name: 销售的产品名称
    :param sold_at: 销售时间点
    :param quantity: 销售的产品数量
    :param prodQ: 产品库存队列
    :return:
    """
    # tmpQ = ordersqueue()
    # ordsDic = tmpQ.buildordersQ()
    # prodQ = ordsDic[name]  # 产品库存队列
    batchlist = []
    for p in prodQ:
        # print p['ordered_at'], sold_at, p['ordered_at'] <= sold_at
        if p['ordered_at'] <= sold_at:          # 加入时间筛选条件:销售时间应该在进货时间之后，否则说明库存数据缺失
            if quantity > p['quantity']:
                batchlist.append(p)
                quantity -= p['quantity']
                prodQ.remove(p)
            elif quantity < p['quantity']:
                batchlist.append(p)
                p['quantity'] -= quantity
                return batchlist
            else:
                batchlist.append(p)
                prodQ.remove(p)
                return batchlist
        else:
            print "Warning:产品库存数据错误,可能缺少进货数据:", p['name'],'\n'
            # sys.exit(1)
            return []
    pass


def bc_traceability(salepoint, prtflag=False):
    """
        从消费者环节往回溯源
    :param salepoint: 销售地点（"常州分公司总仓"或"太仓分公司总仓"）
    :param prfflag： 打印输出标记
    :return: 返回bsTraceDic溯源字典，结构为:
            {
                "消费者( (消费产品)__(消费时间) )":
                    [
                        [供应商节点列表],
                        销售地点（分公司）,
                        消费者代号(c_x)
                    ],
                    [ ...], ...
            }
    """

    tmpconn = mongops()

    sale_repo = salepoint
    # 获取销售列表
    res = tmpconn.db["sales_collection"].find({"sale_repertory": sale_repo},
                                              {'name': 1, 'sold_by': 1, 'sold_at': 1, 'sale_repertory': 1,
                                               'quantity': 1}).sort("sold_at", pymongo.ASCENDING)
    # 建立某个分公司各个产品库存队列
    tmpQ = ordersqueue()
    ordsDic = tmpQ.buildordersQ(sale_repo)

    bsTraceDic = {}
    for i, k in enumerate(res):
        # print i,k
        # node_c = "c_%s__(%s)" % (str(i), k["sold_at"])  # 消费者名称
        node_c = "%s__(%s)" % (k['name'], k["sold_at"])  # 消费者名称
        node_c_code = "c_%s" % i
        node_b = tmpconn.getsalepointname(k["sold_by"])
        # print node_b, node_c
        # print ordsDic[k['name'].encode('utf-8')]
        if k['name'] not in ordsDic.keys():
            print u"没有产品: [%s]的进货信息" % k['name']
        else:
            node_s_list = getsaler(k['name'], k["sold_at"], k['quantity'], ordsDic[k['name']])  # 获得消费者购买产品对应的在仓库中的批次信息
            if node_s_list:
                # supplylist = [p["supplier_id"] if p["supplier_id"] else (p['name'] + "_" + p["ordered_at"]) for p in node_s_list]
                supplylist = ["%s__(%s)" % (p['name'], p["ordered_at"]) for p in node_s_list]
                if prtflag: print ",".join(supplylist),'    ', node_b,'    ',  node_c
                bsTraceDic[node_c] = [supplylist, node_b, node_c]
    return bsTraceDic


def prod2consumer(salepoint, bstraceDic=None, prtflag=False):
    """
        根据库存中的产品批次溯源消费者的分布
        思路：基于BC_tracibility的最终结果中的批次信息（产品名称_买入时间），在使用批次信息依次去上述结果中比对，找到所有对应的consumer
    :param salepoint: 销售点名称（"常州分公司总仓"或"太仓分公司总仓"）
    :return: 返回溯源路径字典，格式如下：
        {
        "批次信息（产品名称__进货时间）":
                    [
                        消费者（c_x）__消费时间,
                        ...
                    ]
        }
    """
    if not bstraceDic:
        bstraceDic = bc_traceability(salepoint=salepoint)       # 反向溯源路径的字典
    fwdtraceDic = {}
    for k, v in bstraceDic.items():
        batchlist = v[0]  # 销售产品涉及的批次信息
        for b in batchlist:
            if b not in fwdtraceDic.keys():
                fwdtraceDic[b] = [k, ]
            else:
                fwdtraceDic[b].append(k)
    if prtflag:
        for k, v in fwdtraceDic.items():
            print salepoint, k, ' ：', v
    return fwdtraceDic

def getconsumername(pname, ptime):
    """
        根据产品名称和消费时间，合成反向溯源路径字典中的key

    :return:
    """
    return U"%s__(%s)" % (pname, ptime)

def parsenode(pathinfo, mode):
    """
    pathinfo格式：
        1. mode = 1:
        {
                    "消费者( (消费产品)__(消费时间) )":
                        [
                            [供应商节点列表(产品名称__出厂日期)],
                            销售地点（分公司）,
                            消费者代号(c_x)
                        ],
                        [ ...], ...
        }
        2. mode = 2:
        {
            "批次信息（产品名称__进货时间）":
                        [
                            消费者（c_x）__消费时间,
                            ...
                        ]
        }

    :return:
    """
    if mode == 1:
        prodlist = [p.split('__') for p in pathinfo[0]]
        repo = pathinfo[1]
        consumer_info = pathinfo[2].split('__')
        return [prodlist, repo, [consumer_info],]
    elif mode == 2:
        return [p.split('__') for p in pathinfo]



def prettyshow(pathinfo):
    """
        解析溯源路径，并输出
    :return:
    """
    prodtnames = pathinfo[0]
    repo = pathinfo[1]
    consumers = pathinfo[2]
    print "生产厂家:"
    for i in prodtnames:
        print u"产品名称:%s, 出厂日期:%s" % (i[0], i[1])
    print "\n销售地点: %s" % repo
    print "\n消费者:"
    for c in consumers:
        print u"购买的产品:%s, 购买时间:%s" % (c[0], c[1])

def run(mode, salepoint, pname, ptime):
    """
    说明： 溯源程序入口
    参数：
    :param mode: 溯源的模式：
                mode=1： 反向溯源（以消费者为起点）
                mode=2： 正向溯源（以出厂或库存产品为起点）
    :param salepoint: 销售地点（"常州分公司总仓"或"太仓分公司总仓"）
    :param pname: 产品名称
    :param ptime: 时间
            mode=1：消费者购买产品时间
            mode=2：产品出厂或者入库的时间
    :return:返回值格式
            [
                厂家信息:[[产品名称，出厂时间],...],
                销售点:太仓分公司总仓/常州分公司总仓,
                消费者信息:[(消费产品名称，消费时间),...]
            ]
    """
    bstraceDic = bc_traceability(salepoint=salepoint)  # 反向溯源路径的字典
    pathkey = getconsumername(pname, ptime)
    if mode == 1:
        pathinfo = bstraceDic[pathkey]
        pathinfo = parsenode(pathinfo=pathinfo, mode=mode)
        # pathinfo[2] = [pathinfo[2], pname, ptime]  # consumer_code, 消费产品名称，消费时间
        return pathinfo

    elif mode == 2:
        fwdtracedic = prod2consumer(salepoint=salepoint, bstraceDic=bstraceDic)
        pathinfo = fwdtracedic[pathkey]
        pathinfo = parsenode(pathinfo=pathinfo, mode=mode)
        return [[[pname, ptime],], salepoint, pathinfo]


if __name__ == '__main__':
    bc_traceability(salepoint="太仓分公司总仓", prtflag=True)        # 从消费者开始往回溯源

    # bc_traceability(salepoint="常州分公司总仓", prtflag=True)

    # prod2consumer(salepoint="太仓分公司总仓",prtflag=True) # 从商品往消费者方向溯源

    # prettyshow(run(2, "太仓分公司总仓",U"J三宁硫基复合肥",U"2017-12-31"))

    pass

