# coding:utf-8
__author__ = 'wlw'
import pandas as pd
from os import walk
from os.path import join
from pymongo import MongoClient


def insertcommodity(path):
    collection = db.commodity_collection
    # when the collection is not empty
    if collection.find_one() != None:
        cursor = collection.find().sort([('_id', -1)]).limit(1)  # DESCENDING
        commodity_id = -1
        for record in cursor:
            commodity_id = record['_id'] + 1

        print 'next_commodity_id: ', commodity_id
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        # print df
        commodity_df = df.loc[:, [u'商品名称', u'供应商编码', u'商品类型']].drop_duplicates()
        # print commodity_df.to_dict('records')
        for item in commodity_df.to_dict('records'):
            # record = {'_id':commodity_id, 'name':item[u'商品名称'], 'supplier_id':item[u'供应商编码'], 'type':item[u'商品类型']}
            collection.update({'name':item[u'商品名称']},
                              {'$setOnInsert':{'_id':commodity_id, 'supplier_id':item[u'供应商编码'], 'type':item[u'商品类型']}},
                              upsert=True)
            commodity_id += 1

    # when the collection is empty
    else:
        commodity_id = 1
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        # print df
        commodity_df = df.loc[:, [u'商品名称', u'供应商编码', u'商品类型']].drop_duplicates()
        # print commodity_df.to_dict('records')
        commodities = []
        for item in commodity_df.to_dict('records'):
            record = {'_id':commodity_id, 'name':item[u'商品名称'], 'supplier_id':item[u'供应商编码'], 'type':item[u'商品类型']}
            commodities.append(record)
            commodity_id += 1

        collection.insert_many(commodities)


def insertsuppliers(path):
    collection = db.suppliers_collection
    if collection.find_one() != None:
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        # print df
        suppliers_df = df.loc[:, [u'供应商编码', u'供应商名称', u'供应商地址', u'经纬度']].drop_duplicates()
        for item in suppliers_df.to_dict('records'):
            collection.update({'_id':item[u'供应商编码']},
                              {'$setOnInsert':{'name':item[u'供应商名称'], 'address':item[u'供应商地址'], 'location':[item[u'经纬度']]}},
                              upsert=True)
    else:
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        suppliers_df = df.loc[:, [u'供应商编码', u'供应商名称', u'供应商地址', u'经纬度']].drop_duplicates()
        suppliers = []
        for item in suppliers_df.to_dict('records'):
            loc_str = item[u'经纬度'].split(',')
            loc = [float(loc_str[0]), float(loc_str[1])]
            record = {'_id':item[u'供应商编码'], 'name':item[u'供应商名称'], 'address':item[u'供应商地址'], 'location':loc}
            suppliers.append(record)

        collection.insert_many(suppliers)


def insertorders(path):
    collection = db.orders_collection
    if collection.find_one() != None:
        cursor = collection.find().sort([('_id', -1)]).limit(1)  # DESCENDING
        orders_id = -1
        for record in cursor:
            orders_id = record['_id'] + 1

        print 'next_order_id: ', orders_id
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        orders_df = df.loc[:, [u'商品名称', u'供应商编码', u'商品类型', u'合同数量|合计', u'合同金额', u'订货部门编码', u'订货日期']]

        orders = []
        for item in orders_df.to_dict('records'):
            date = str(item[u'订货日期'])
            y = date[:4]
            m = date[4:6]
            d = date[6:]
            date = y + '-' + m + '-' + d
            record = {'_id':orders_id, 'name':item[u'商品名称'], 'supplier_id':item[u'供应商编码'], 'type':item[u'商品类型'],
                      'quantity':item[u'合同数量|合计'], 'total_amount':item[u'合同金额'], 'ordered_by':item[u'订货部门编码'],
                      'ordered_at':date}

            orders.append(record)
            orders_id += 1

        collection.insert_many(orders)
    else:
        orders_id = 1
        df = pd.read_excel(path, sheetname=u'1-4月采购订单')
        orders_df = df.loc[:, [u'商品名称', u'供应商编码', u'商品类型', u'合同数量|合计', u'合同金额', u'订货部门编码', u'订货日期']]

        orders = []
        for item in orders_df.to_dict('records'):
            date = str(item[u'订货日期'])
            y = date[:4]
            m = date[4:6]
            d = date[6:]
            date = y + '-' + m + '-' + d
            record = {'_id':orders_id, 'name':item[u'商品名称'], 'supplier_id':item[u'供应商编码'], 'type':item[u'商品类型'],
                      'quantity':item[u'合同数量|合计'], 'total_amount':item[u'合同金额'], 'ordered_by':item[u'订货部门编码'],
                      'ordered_at':date}
            orders.append(record)
            orders_id += 1

        collection.insert_many(orders)


def handleorders(path):
    print 'order path: ', path
    # insertcommodity(path)
    # insertsuppliers(path)
    # insertorders(path)


def handlesales(path):
    print 'sale path: ', path
    collection = db.sales_collection
    for sheetname in [u'1月', u'2月', u'3月', u'4月']:
        if collection.find_one() != None:
            cursor = collection.find().sort([('_id', -1)]).limit(1)  # DESCENDING
            sales_id = -1
            for record in cursor:
                sales_id = record['_id'] + 1

            print 'next_sale_id: ', sales_id
            df = pd.read_excel(path, sheetname=sheetname)
            sales_df = df.loc[:, [u'商品名称', u'部门编码', u'销货仓库名称', u'销售数量|整件合计', u'销售单价', u'销货金额', u'入账日期', u'销售时刻']]

            sales = []
            for item in sales_df.to_dict('records'):
                date = item[u'入账日期'].to_pydatetime().strftime('%Y-%m-%d') + ' ' + str(item[u'销售时刻'])
                record = {'_id':sales_id, 'name':item[u'商品名称'], 'sold_by':item[u'部门编码'], 'sale_repertory':item[u'销货仓库名称'],
                          'quantity':item[u'销售数量|整件合计'], 'unit_price':item[u'销售单价'], 'total_amount':item[u'销货金额'],
                          'sold_at':date}
                sales_id += 1
                sales.append(record)

            collection.insert_many(sales)

        else:
            sales_id = 1
            df = pd.read_excel(path, sheetname=sheetname)
            sales_df = df.loc[:, [u'商品名称', u'部门编码', u'销货仓库名称', u'销售数量|整件合计', u'销售单价', u'销货金额', u'入账日期', u'销售时刻']]

            sales = []
            for item in sales_df.to_dict('records'):
                # print 'date2: ', item[u'入账日期'].to_pydatetime().strftime('%Y-%m-%d')
                date = item[u'入账日期'].to_pydatetime().strftime('%Y-%m-%d') + ' ' + str(item[u'销售时刻'])
                # print 'date: ', date

                record = {'_id':sales_id, 'name':item[u'商品名称'], 'sold_by':item[u'部门编码'], 'sale_repertory':item[u'销货仓库名称'],
                          'quantity':item[u'销售数量|整件合计'], 'unit_price':item[u'销售单价'], 'total_amount':item[u'销货金额'],
                          'sold_at':date}
                sales_id += 1
                sales.append(record)

            collection.insert_many(sales)



if __name__ == '__main__':
    pd.set_option('display.width', 1000)

    client = MongoClient("localhost", 27017)
    db = client.envdata_db

    path = '/home/wlw/oliverData/nongzi_sales/'
    for (dirpath, dirnames, filenames) in walk(path):
        if len(filenames) > 0:
            for f in filenames:
                if dirpath.split('/')[-1] == 'orders':
                    #handleorders(join(dirpath, f))
                    pass
                elif dirpath.split('/')[-1] == 'sales':
                    handlesales(join(dirpath, f))


