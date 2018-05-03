#!/usr/bin/python
# -*- coding: utf-8 -*-

# Created by David Teng on 18-5-1

import sys,pprint
from gexf import Gexf
from BC_traceability import *

"""
    代码模板：
    # test helloworld.gexf
    gexf = Gexf("Gephi.org", "A Back traceability network")
    graph = gexf.addGraph("directed", "static", "start from consumer")

    atr1 = graph.addNodeAttribute('url', defaultValue='sunong.org',type='string')
    atr2 = graph.addNodeAttribute('indegree', defaultValue='0', type='float')
    atr3 = graph.addNodeAttribute('frog', type='boolean', defaultValue='true')

    tmp = graph.addNode("0", "Gephi")
    tmp.addAttribute(atr1, "http://gephi.org")
    tmp.addAttribute(atr2, '1')

    tmp = graph.addNode("1", "Webatlas")
    tmp.addAttribute(atr1, "http://webatlas.fr")
    tmp.addAttribute(atr2, '2')

    tmp = graph.addNode("2", "RTGI")
    tmp.addAttribute(atr1, "http://rtgi.fr")
    tmp.addAttribute(atr2, '1')

    tmp = graph.addNode("3", "BarabasiLab")
    tmp.addAttribute(atr1, "http://barabasilab.com")
    tmp.addAttribute(atr2, '1')
    tmp.addAttribute(atr3, 'false')

    graph.addEdge("0", "0", "1", weight='1')
    graph.addEdge("1", "0", "2", weight='1')
    graph.addEdge("2", "1", "0", weight='1')
    graph.addEdge("3", "2", "1", weight='1')
    graph.addEdge("4", "0", "3", weight='1')

    output_file = open("./data.gexf", "w")
    gexf.write(output_file)
"""

def mkgexfile(filepath, pathinfo):
    gexf = Gexf("NJFE_wlw", "A traceability network")
    graph = gexf.addGraph("directed", "", "Back or Foward traceability")

    atr1 = graph.addNodeAttribute('commodity_name', mode="dynamic",defaultValue='', type='string', force_id="commodity_name")
    atr2 = graph.addNodeAttribute('time', mode="dynamic", defaultValue='', type='string', force_id="time")
    atr3 = graph.addNodeAttribute('nodename', mode="dynamic", type='string', defaultValue='', force_id="nodename")   # 节点名称
    atr4 = graph.addNodeAttribute('nodetype', mode="dynamic", type='integer', defaultValue='', force_id="nodetype")  # supplier、repertory和consumer
    # atr5 = graph.addNodeAttribute('')

    prodtnames = pathinfo[0]        # 产品出厂信息： 产品名称、出厂时间、（还需要补充商家信息）
    repo = pathinfo[1]              # 产品仓库信息：仓库名称、入库时间
    consumers = pathinfo[2]         # 消费者信息：购买的产品名称、购买的时间

    # 厂家节点
    for i, p in enumerate(prodtnames):
        tmp = graph.addNode('s_'+`i`, "suppliers_%s" % i)
        tmp.addAttribute(atr1, p[0])    # 产品名称
        tmp.addAttribute(atr2, p[1])    # 出厂日期
        tmp.addAttribute(atr4, "0")      # 节点类型:"supplier"

    # 仓库节点(销售地点)
    tmp = graph.addNode("R_1", "Repertory")
    tmp.addAttribute(atr4, "1")   # "repetory"
    tmp.addAttribute(atr3, repo)


    for i,c in enumerate(consumers):
        tmp = graph.addNode("c_"+`i`, "consumer_%s" % i)
        tmp.addAttribute(atr1, c[0])  # 产品名称
        tmp.addAttribute(atr2, c[1])  # 消费者购买日期
        tmp.addAttribute(atr3, "consumer_%s" % i)   # 节点名称
        tmp.addAttribute(atr4, "2")  # 节点类型 "consumer")

    # print "finished nodes"
    for i, _ in enumerate(prodtnames):
        graph.addEdge(`i`, 's_'+`i`, "R_1", weight='1')


    skiplen = len(prodtnames)

    for i, _ in enumerate(consumers):
        graph.addEdge(`(i+skiplen)`, "R_1", "c_"+`i`, weight='1')
    # print "finied edges"

    output_file = open(filepath, "w")
    gexf.write(output_file)
    pass

if __name__ == '__main__':
    # pathinfo = run(2, u"太仓分公司总仓", u"J三宁硫基复合肥", u"2017-12-31")
    pathinfo = run(1, u"太仓分公司总仓", u"J40%三宁高塔硫基（22-9-9）", u"2018-04-10 16:54:27")
    filepath = "/home/david/Documents/LAB_Codes/Flask/flask-gentelella/source/base/static/images/BKTdata.gexf"
    mkgexfile(filepath, pathinfo)
    pass
