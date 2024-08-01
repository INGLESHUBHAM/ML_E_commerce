# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
#import s3fs
#import psycopg2

from flask import Flask,request
import json


application = Flask(__name__)

#con=psycopg2.connect(dbname= 'dev', host='mantix-cluster.cmpdmnkfrh2m.us-east-2.redshift.amazonaws.com',
#port= '5439', user= 'mantix', password= 'Mantix!2')

#cur = con.cursor()

class RFM_Clustering():

    def __init__(self,data):
        self.data=data.copy()

    #pull required data
    def Data_pull(self,df,date_column_name,start_date,end_date,tenant_id,store_id):

        df[date_column_name]=pd.to_datetime(df[date_column_name])
       #greater than the start date and smaller than the end date
        mask = (df[date_column_name] > start_date) & (df[date_column_name] <= end_date)
        df_filter=df.loc[mask]
        #df_filter=df_filter[(df_filter['tenant_id']==tenant_id) & (df_filter['store_id']==store_id)]
        df_filter=df_filter[['customer_account_id','submitted_date','total','product_code','quantity']]

        return df_filter

   #prepare features
    def Features_pull(self,data, groupby_cols=['customer_account_id'], unit_cols='total',date_col='submitted_date',end_date='2020-12-31'):

        def recency(x, today=end_date):
            #print(f"today : {pd.to_datetime(today)} | x : {x.max()} | Recency : {(pd.to_datetime(today) - x.max()).days}")
            return (pd.to_datetime(today) - x.max()).days

        def ntransaction(x):
            #print(f'Count : {x.count()} | nunique : {x.nunique()}')
            return x.count()

        def tenure(x):
            return (x.max() - x.min()).days

        def avg_duration_bet_purchase(x):
            x = x.sort_values()
            x = (x - x.shift())
            x = x.mean()
            return 0 if pd.isnull(x) else x.days

        #print('Doing Groupby !')
        df_grp = data.groupby(groupby_cols).agg({date_col:[recency,avg_duration_bet_purchase,tenure,ntransaction],
                                                     unit_cols:['sum'],'total':['sum'],'product_code':[ntransaction]})
        df_grp.columns = ['_'.join(x) for x in df_grp.columns.ravel()]
        df_grp.reset_index(inplace=True)

        df_grp = df_grp.replace([np.inf, -np.inf], np.nan)
        df_grp.fillna(0,inplace=True)

        return df_grp

    def RFM_score_getter(self,df,no_quantiles,labels=[1,2,3,4,5]):

        def get_dict(dic,reverse=False):
            dic =  dict(sorted(dic.items(),key=lambda x : x[0].left,reverse=reverse))
            i=1
            for k in dic.keys():
                dic[k] = i
                i+=1
            return dic
        def get_label(x,dic):
            for key,value in dic.items():
                if x > key.left and x<= key.right:
                    return value


        recency_dict =get_dict(pd.qcut(df['Recency'],q=no_quantiles,duplicates='drop').value_counts().to_dict(),reverse=False)
        frequency_dict =get_dict(pd.qcut(df['Frequency'],q=no_quantiles,duplicates='drop').value_counts().to_dict(),reverse=True)
        margin_dict =get_dict(pd.qcut(df['Revenue'],q=no_quantiles,duplicates='drop').value_counts().to_dict(),reverse=True)

        df['R'] = df['Recency'].apply(lambda x : get_label(x,recency_dict))
        df['F'] = df['Frequency'].apply(lambda x : get_label(x,frequency_dict))
        df['M'] = df['Revenue'].apply(lambda x : get_label(x,margin_dict))

        df['R']=df['R'].astype('int')
        df['F']=df['F'].astype('int')
        df['M']=df['M'].astype('int')
        #Calculate and Add RFMGroup value column showing combined concatenated score of RFM
        df['RFMGroup'] = df.R.map(str) + df.F.map(str) + df.M.map(str)

        #Calculate and Add RFMScore value column showing total sum of RFMGroup values
        df['RFMScore'] = df[['R', 'F', 'M']].sum(axis = 1)

        df['RFMGroup_name']=df['RFMGroup'].map({'111':'Champions','113':'Promising','114':'Potential Loyalist','144':'New Customers',
    '312':'Loyal','442':'Cant loose them','443':'Need Attention' ,'552':'About to Sleep',
     '553':'Hibernating','554':'At Risk','555':'Lost'})



        df['sort']=df['RFMGroup_name'].map({'Loyal':1,'Champions':2,'Potential Loyalist':3,'New Customers':4,'Promising':5,'Need Attention':6,
        'About to Sleep':7,'Hibernating':8,'Cant loose them':9,'At Risk':10,'Lost':11})

        #Assign Loyalty Level to each customer
        Loyalty_Level = ['Platinum', 'Gold', 'Silver', 'Bronze']
        Score_cuts = pd.qcut(df.RFMScore, q = 4, labels = Loyalty_Level)
        df['RFM_Loyalty_Level'] = Score_cuts.values


        return df


@application.route("/")
def index():
    return "Hello from RFMB! Please use /rfm url"

@application.route("/rfm",methods=["POST"])
def rfm():
    try:
        #if request.is_json:
        #data = {'CampaignType':'Email'}
        #print(request.method)
        #print(request.get_json())
        input_data = request.get_json()
        #print(f'input_data : {input_data}')
        #input_data = {start_data='2017-09-13',end_date='2021-12-18','recencyOptions':20,'monetizationOptions':200,frequencyOptions:1}

        # setup start_data from input_data
        if 'startDate' in input_data.keys() and len(input_data['startDate'])==10:
            start_date=input_data['startDate']
        else:
            start_date='2017-09-13'

        #setup end_date from input_data
        if 'endDate' in input_data.keys() and len(input_data['endDate'])==10:
            end_date=input_data['endDate']
        else:
            end_date='2021-12-18'

        #setup recency threshold from input_data
        if 'recencyOptions' in input_data and len(str(input_data['recencyOptions']))>0:
            if str(input_data['recencyOptions']).isdigit():
                recency_th=float(input_data['recencyOptions'])
            else:
                return json.dumps({'Error' : 'recencyOptions must be an integer value'})
        else:
            recency_th=-1

        #setup Revenue threshold from input_data
        if 'monetizationOptions' in input_data and len(str(input_data['monetizationOptions']))>0:
            if str(input_data['monetizationOptions']).isdigit():
                revenue_th=float(input_data['monetizationOptions'])
            else:
                return json.dumps({'Error' : 'monetizationOptions must be an integer value'})
        else:
            revenue_th=-1

        #setup Frequency threshold from input_data
        if 'frequencyOptions' in input_data and len(str(input_data['frequencyOptions']))>0:
            if str(input_data['frequencyOptions']).isdigit():
                frequency_th=float(input_data['frequencyOptions'])
            else:
                return json.dumps({'Error' : 'frequencyOptions must be an integer value'})
        else:
            frequency_th=-1

        current_dir = os.getcwd()
        #pull item data from the DB
        #items_data = pd.read_sql("select * from cdp.items_data;",con)
        items_data = pd.read_csv(current_dir+'/items_data.csv')
        #items_data = pd.read_csv('s3://rfmb-data/items_data.csv')
        items_data = items_data[['product_code','order_number','quantity']].copy()

        # Read order table from the csv file
        #b = pd.read_sql("select * from cdp.orders_data;",con)
        b = pd.read_csv(current_dir+'/orders_data.csv')
        b = b[~b['status'].isin(['Abandoned','Errored','Pending','PendingReview','Cancelled','Null'])]

        # merge order and item data
        b.order_number = b.order_number.astype(int).astype(str)
        items_data.order_number = items_data.order_number.astype(int).astype(str)
        b =pd.merge(b,items_data,on=['order_number'],how='left')
        del items_data
        # Start to create RFMB features
        RFM=RFM_Clustering(b)

        tenant_id='TNB00084'
        store_id = 'STOM000000123'
        #start_date = '2017-09-13'
        #end_date='2021-12-18'

        #del b
        df_final=RFM.Data_pull(b,date_column_name='submitted_date',start_date=start_date,end_date=end_date,tenant_id=tenant_id,store_id = store_id)
        del b
        df_features=RFM.Features_pull(df_final,end_date=df_final.submitted_date.max())
        del df_final
        df_features.columns=['cust_id','Recency','avg_duration','Tenure','Frequency','Revenue','Breadth']

        RFM_features=df_features[['cust_id','Recency','Frequency','Revenue']].copy()

        del df_features

        if recency_th !=-1:
            RFM_features=RFM_features[RFM_features['Recency']<=recency_th]
        if frequency_th !=-1:
            RFM_features=RFM_features[RFM_features['Frequency']<=frequency_th]
        if revenue_th !=-1:
            RFM_features=RFM_features[RFM_features['Revenue']<=revenue_th]

        RFM_scores=RFM.RFM_score_getter(RFM_features,no_quantiles=5,labels=[1,2,3,4,5])
        del RFM_features
        report=RFM_scores.groupby(['sort','RFMGroup_name']).agg({'cust_id':'count'}).reset_index()
        del RFM_scores
        report['percentage'] = (report['cust_id'] / report['cust_id'].sum()) * 100

        report['percentage']=np.round(report['percentage'],decimals=2).astype(str)
        #print('('+report['precentage']+'%)')

        report=report.rename(columns={'cust_id':'count','RFMGroup_name':'label'})
        
        report['label'] = np.where(report['label']=='Loyal Customers','Loyal',report['label'])

        res = report.to_json(orient='records', lines=False)
        res  = json.loads(res)

        res = {'rmf_data':res}

        for r in res['rmf_data']:
            #print(r['percentage'])
            r['percentage']=str('(')+str(r['percentage'])+str('%)')

        print(f'res : {res}')
        return json.dumps(res)
    except Exception as e:
        #print(str(e))
        return json.dumps({'Error '+str(e)})
    
@application.route("/rfm/customer",methods=["POST"])
def rfm_customer():
    try:
        #if request.is_json:
        #data = {'CampaignType':'Email'}
        #print(request.method)
        #print(request.get_json())
        
        input_data = request.get_json()
        #print(f'input_data : {input_data}')
        #input_data = {start_data='2017-09-13',end_date='2021-12-18','recencyOptions':20,'monetizationOptions':200,frequencyOptions:1}
        
        cust_label = input_data.get("label","")
        
        # setup start_data from input_data
        if 'startDate' in input_data.keys() and len(input_data['startDate'])==10:
            start_date=input_data['startDate']
        else:
            start_date='2017-09-13'

        #setup end_date from input_data
        if 'endDate' in input_data.keys() and len(input_data['endDate'])==10:
            end_date=input_data['endDate']
        else:
            end_date='2021-12-18'

        #setup recency threshold from input_data
        if 'recencyOptions' in input_data and len(str(input_data['recencyOptions']))>0:
            if str(input_data['recencyOptions']).isdigit():
                recency_th=float(input_data['recencyOptions'])
            else:
                return json.dumps({'Error' : 'recencyOptions must be an integer value'})
        else:
            recency_th=-1

        #setup Revenue threshold from input_data
        if 'monetizationOptions' in input_data and len(str(input_data['monetizationOptions']))>0:
            if str(input_data['monetizationOptions']).isdigit():
                revenue_th=float(input_data['monetizationOptions'])
            else:
                return json.dumps({'Error' : 'monetizationOptions must be an integer value'})
        else:
            revenue_th=-1

        #setup Frequency threshold from input_data
        if 'frequencyOptions' in input_data and len(str(input_data['frequencyOptions']))>0:
            if str(input_data['frequencyOptions']).isdigit():
                frequency_th=float(input_data['frequencyOptions'])
            else:
                return json.dumps({'Error' : 'frequencyOptions must be an integer value'})
        else:
            frequency_th=-1

        current_dir = os.getcwd()
        #pull item data from the DB
        #items_data = pd.read_sql("select * from cdp.items_data;",con)
        items_data = pd.read_csv(current_dir+'/items_data.csv')
        #items_data = pd.read_csv('s3://rfmb-data/items_data.csv')
        items_data = items_data[['product_code','order_number','quantity']].copy()
        
        # Read product datafrom csv file
        products_data = pd.read_csv(current_dir+'/products_data.csv')
        products_data = products_data[products_data['product_usage'].isin(['Standard'])]

        # Read order table from the csv file
        #b = pd.read_sql("select * from cdp.orders_data;",con)
        b = pd.read_csv(current_dir+'/orders_data.csv')
        b = b[~b['status'].isin(['Abandoned','Errored','Pending','PendingReview','Cancelled','Null'])]

        # merge order and item data
        b.order_number = b.order_number.astype(int).astype(str)
        items_data.order_number = items_data.order_number.astype(int).astype(str)
        b =pd.merge(b,items_data,on=['order_number'],how='left')
        b =pd.merge(b,products_data,on=['product_code'],how='left')
        
        del items_data
        del products_data
        # Start to create RFMB features
        RFM=RFM_Clustering(b)

        tenant_id='TNB00084'
        store_id = 'STOM000000123'
        #start_date = '2017-09-13'
        #end_date='2021-12-18'

        #del b
        df_final=RFM.Data_pull(b,date_column_name='submitted_date',start_date=start_date,end_date=end_date,tenant_id=tenant_id,store_id = store_id)
        del b
        df_features=RFM.Features_pull(df_final,end_date=df_final.submitted_date.max())
        del df_final
        df_features.columns=['cust_id','Recency','avg_duration','Tenure','Frequency','Revenue','Breadth']

        RFM_features=df_features[['cust_id','Recency','Frequency','Revenue']].copy()

        del df_features

        if recency_th !=-1:
            RFM_features=RFM_features[RFM_features['Recency']<=recency_th]
        if frequency_th !=-1:
            RFM_features=RFM_features[RFM_features['Frequency']<=frequency_th]
        if revenue_th !=-1:
            RFM_features=RFM_features[RFM_features['Revenue']<=revenue_th]

        RFM_scores=RFM.RFM_score_getter(RFM_features,no_quantiles=5,labels=[1,2,3,4,5])
        del RFM_features
        
        RFM_scores_label = RFM_scores[RFM_scores['RFMGroup_name']==cust_label]
        
        customer_id = RFM_scores_label['cust_id'].astype(int).astype(int).values.tolist()
        
        print(f"Customer ids : {customer_id}")
        
        
        res = {'label':cust_label,
               'customer_id':customer_id}
        
        #res  = json.loads(res)

        res = {'rmf_customer_data':res}

        print(f'res : {res}')
        return json.dumps(res)
    except Exception as e:
        #print(str(e))
        return json.dumps({'Error '+str(e)})

if __name__ == "__main__":
    application.run(host="0.0.0.0")
