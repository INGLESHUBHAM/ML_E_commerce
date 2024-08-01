import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import operator
from datetime import date
import psycopg2



def cust_recommendation():
    
    con=psycopg2.connect(dbname= 'dev', host='mantix-cluster.cgzkthavydhk.us-east-2.redshift.amazonaws.com', 
    port= '5439', user= 'mantix', password= 'Mantix123!')
    cur = con.cursor()
    items_data = pd.read_sql("select * from cdp.items_data;",con)
    orders_data = pd.read_sql("select * from cdp.orders_data;",con)
    orders_data = orders_data[orders_data.status.isin(['Abandoned','Pending'])]
    orders_data = orders_data.sort_values(by='fulfillment_date',ascending=False).head(9000)

    # Customer based recommendation
    orders_data = orders_data[['order_number','customer_account_id','tenant_id','store_id']]
    orders_data = orders_data.dropna()
    items_data = items_data[['order_number','product_code','quantity']]


    orders_data.order_number = orders_data.order_number.astype(int).astype(str)
    items_data.order_number = items_data.order_number.astype(int).astype(str)

    orders_data =pd.merge(orders_data,items_data,on=['order_number'],how='left')
    orders_data = orders_data.dropna()
    del items_data

    pivot_df = pd.pivot_table(orders_data,index='customer_account_id',columns='product_code',values='quantity',aggfunc='sum')
    pivot_df = pivot_df.fillna(0)


    def similar_users(user_id, matrix, k=3):
        # create a df of just the current user
        user = matrix[matrix.index == user_id]
        
        # and a df of all other users
        other_users = matrix[matrix.index != user_id]
        
        # calc cosine similarity between user and each other user
        similarities = cosine_similarity(user,other_users)[0].tolist()
        
        # create list of indices of these users
        indices = other_users.index.tolist()
        
        # create key/values pairs of user index and their similarity
        index_similarity = dict(zip(indices, similarities))
        
        # sort by similarity
        index_similarity_sorted = sorted(index_similarity.items(), key=operator.itemgetter(1))
        index_similarity_sorted.reverse()
        
        # grab k users off the top
        top_users_similarities = index_similarity_sorted[:k]
        users = [u[0] for u in top_users_similarities]
        
        return users

    #Now write a function to make the recommendation. We’ve set the function to return the 10 top recommended 
    def recommend_item(user_index, similar_user_indices, matrix, items=10):
        # load vectors for similar users
        similar_users = matrix[matrix.index.isin(similar_user_indices)]
        # calc avg ratings across the 3 similar users
        similar_users = similar_users.mean(axis=0)
        # convert to dataframe so its easy to sort and filter
        similar_users_df = pd.DataFrame(similar_users, columns=['mean'])
        
        
        # load vector for the current user
        user_df = matrix[matrix.index == user_index]
        # transpose it so its easier to filter
        user_df_transposed = user_df.transpose()
        # rename the column as 'rating'
        user_df_transposed.columns = ['rating']
        # remove any rows without a 0 value. Anime not watched yet
        user_df_transposed = user_df_transposed[user_df_transposed['rating']==0]
        # generate a list of animes the user has not seen
        animes_unseen = user_df_transposed.index.tolist()
        
        # filter avg ratings of similar users for only anime the current user has not seen
        similar_users_df_filtered = similar_users_df[similar_users_df.index.isin(animes_unseen)]
        # order the dataframe
        similar_users_df_ordered = similar_users_df.sort_values(by=['mean'], ascending=False)
        # grab the top n anime   
        top_n_anime = similar_users_df_ordered.head(items)
        top_n_anime_indices = top_n_anime.index.tolist()
        # lookup these anime in the other dataframe to find names
        #recommend_info = orders_data[orders_data['product_code'].isin(top_n_anime_indices)]
        recommend_product = ','.join([str(int(prod)) for prod in top_n_anime_indices])
        return recommend_product #items

    def start_recommedation(customer):
        similar_user_indices = similar_users(customer, pivot_df, k=3)
        recommend_prod_ids = recommend_item(customer, similar_user_indices, pivot_df)
        return recommend_prod_ids

    # Define the cutomers to provide the recommendation
    customer_list = list(orders_data.customer_account_id.unique())
    customer_list_df = pd.DataFrame()
    customer_list_df['customer_id'] = customer_list
    print('total customer : ',len(customer_list))
    recommendation_data = pd.DataFrame(columns=['customer_account_id','recommendation_product'])
    for i,customer in enumerate(customer_list):
        if i%50==0:
            print(f'cust_no: {i}',end=' ')
        recommend_prod_ids = start_recommedation(customer)
        recommendation_data.loc[len(recommendation_data)] = [customer,recommend_prod_ids]

    #recommendation_data['customer_account_id']=recommendation_data['customer_account_id'].astype(int).astype(str)
    #orders_data['customer_account_id']=orders_data['customer_account_id'].astype(int).astype(str)

    final_recommendation = pd.merge(recommendation_data,orders_data[['customer_account_id','tenant_id','store_id']],how='inner',on='customer_account_id')
    final_recommendation = final_recommendation.reset_index(drop=True)
    final_recommendation = final_recommendation.drop_duplicates(keep='first')
    con.close()
    return final_recommendation

def cust_abandon_recommendation(final_recommendation):

    con=psycopg2.connect(dbname= 'dev', host='mantix-cluster.cgzkthavydhk.us-east-2.redshift.amazonaws.com', 
    port= '5439', user= 'mantix', password= 'Mantix123!')
    cur = con.cursor()

    # get the required product 
    #abnd_cust_rec = pd.read_csv('abnd_cust_rec.csv')
    final_recommendation = final_recommendation.rename(columns={'recommendation_product':'product_code'})
    abnd_cust_rec = final_recommendation.copy()
    abnd_cust_rec['is_add_prod'] = abnd_cust_rec['product_code'].apply(lambda x : 1 if len(x.split(','))<6 else 0)
    prod_code_req = abnd_cust_rec[abnd_cust_rec.is_add_prod==1]['product_code'].tolist()
    #prod_code_req = [prod.split(',') for prod in prod_code_req]
    prod_code_req  = list(set(','.join(prod_code_req).split(',')))


    items_data = pd.read_sql("select * from cdp.items_data;",con)
    orders_data = pd.read_sql("select * from cdp.orders_data;",con)
    abandon_cust = orders_data[orders_data.status=='Abandoned']

    ord_cust = items_data[['order_number','tenant_id','store_id','product_code','quantity']]
    ord_cust = ord_cust[ord_cust.order_number.isin(orders_data.order_number.tolist())]

    ord_cust1 = ord_cust[ord_cust.quantity>=10]
    ord_cust2 = ord_cust[ord_cust.product_code.isin(prod_code_req)]

    ord_cust = pd.concat([ord_cust1,ord_cust2],ignore_index=True).drop_duplicates(keep='first').reset_index(drop=True)

    pivot_df = pd.pivot_table(ord_cust,index = 'order_number',columns = 'product_code',values = 'quantity')
    #pivot_df.reset_index(inplace=True)
    pivot_df = pivot_df.fillna(0)

    def get_recommendations(df, item):
        """Generate a set of product recommendations using item-based collaborative filtering.
        
        Args:
            df (dataframe): Pandas dataframe containing matrix of items purchased.
            item (string): Column name for target item. 
            
        Returns: 
            recommendations (dataframe): Pandas dataframe containing product recommendations. 
        """
        
        recommendations = df.corrwith(df[item])
        recommendations.dropna(inplace=True)
        recommendations = pd.DataFrame(recommendations, columns=['correlation']).reset_index()
        recommendations = recommendations[recommendations.product_code != item]
        recommendations = recommendations.sort_values(by='correlation', ascending=False)
        
        return recommendations

    # add rec prod
    def get_prod_id(x):
        if x['is_add_prod']==1:
            products = x['product_code'].split(',')
            print(products)
            req_prod = 6 - len(products)
            recommendations = pd.DataFrame()
            for p in products:
                if len(ord_cust[ord_cust.product_code==p])==0:
                    continue
                recommendations = pd.concat([recommendations,get_recommendations(pivot_df,p)],ignore_index=True)
            if len(recommendations) == 0:
                all_prod = products
            else:
                recommendations = recommendations.sort_values(by='correlation', ascending=False)
                rec_prod = recommendations.product_code.tolist()[1:req_prod+1]
                all_prod = products + rec_prod
            return ','.join(all_prod)
        else:
            return x['product_code']
        
    abnd_cust_rec['new_product_code'] = abnd_cust_rec.apply(lambda x : get_prod_id(x),axis=1)

    abnd_cust_rec['is_add_prod'] = abnd_cust_rec['new_product_code'].apply(lambda x : 1 if len(x.split(','))<6 else 0)

    top_prod = ord_cust.groupby(['product_code'])[['product_code']].count().rename(columns={'product_code':'freq'}).reset_index()
    top_prod = top_prod.sort_values(by='freq',ascending=False).head(10)
    top_prod_list = top_prod.product_code.tolist()[:10]
    abnd_cust_rec0 = abnd_cust_rec[abnd_cust_rec.is_add_prod==0]
    abnd_cust_rec1 = abnd_cust_rec[abnd_cust_rec.is_add_prod==1]

    def add_prod(x):
        x = x.split(',') + top_prod_list
        return ','.join(x[:6])
        
    abnd_cust_rec1['new_product_code'] = abnd_cust_rec1['new_product_code'].apply(add_prod)

    abnd_cust_rec = pd.concat([abnd_cust_rec0,abnd_cust_rec1],ignore_index=True)

    abnd_cust_rec['product_code'] = abnd_cust_rec['new_product_code']

    #cust_profile = pd.read_sql("select * from cdp.customers_profile;",con)
    #cust_profile = cust_profile[['customer_id','account_id']]
    #abnd_cust_rec = pd.merge(abnd_cust_rec,cust_profile,left_on='customer_account_id',right_on='customer_id',how='inner')

    abnd_cust_rec = abnd_cust_rec[['customer_account_id','product_code','tenant_id','store_id']]
    today_date = date.today()

    abnd_cust_rec['create_dms'] = today_date
    con.close()
    return abnd_cust_rec

def load_db(df):
    con=psycopg2.connect(dbname= 'dev', host='mantix-cluster.cgzkthavydhk.us-east-2.redshift.amazonaws.com', 
    port= '5439', user= 'mantix', password= 'Mantix123!')
    cur = con.cursor()

    def drop_table(conn,table):
        try:
            query = f"""DROP TABLE IF EXISTS {table}"""
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            print(f'Drop table {table} successfully')
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cur.close()
            #return 1

    def create_user_recommendation_table(conn,table):
        try:
            query = f"""CREATE TABLE {table} (customer_account_id VARCHAR(1000),
                            recommendation_product VARCHAR(5000),
                            tenant_id VARCHAR(1000),
                            store_id VARCHAR(1000),
                            create_dms VARCHAR(1000)
                        )"""
            cur = conn.cursor()
            cur.execute("ROLLBACK")
            conn.commit()
            cur.execute(query)
            conn.commit()
            print(f'Successfully created the table {table}')
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cur.close()
            return 1
            
    def single_insert(conn, insert_req):
        """ Execute a single INSERT request """
        cursor = conn.cursor()
        try:
            cursor.execute(insert_req)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()
        
    def insert_dataframe(df,conn,table):
        for i in df.index:
            query = f"""
            INSERT into {table} (customer_account_id,recommendation_product ,tenant_id ,store_id,create_dms)
                values('%s',%s,%s,%s,%s);
            """ %(df.loc[i]['customer_account_id'],
                "'"+str(df.loc[i]['product_code'])+"'",
                    "'"+str(df.loc[i]['tenant_id'])+"'",
                "'"+str(df.loc[i]['store_id'])+"'",
                "'"+str(df.loc[i]['create_dms'])+"'")
            if i%50==0:
                print(i,end=' ')
            
            # check if customer_id and create_dms is already exist then don't insert
            check_query = f"SELECT * FROM {table} WHERE customer_account_id='{df.loc[i]['customer_account_id']}' and create_dms='{df.loc[i]['create_dms']}'"
            check_insert_df = pd.read_sql(check_query,conn)
            if len(check_insert_df) == 0:
                single_insert(conn, query)
        conn.commit()
        print('\nSuccessfully inserted dataframe into the table')
        
    table = 'cdp.cust_abandoned_carts'
    #drop_table(con,table)
    #create_user_recommendation_table(con,table)
    insert_dataframe(df,con,table)

if __name__ == '__main__':

    # start customer recommendation
    final_recommendation = cust_recommendation()

    # start abandon customer affinity product
    abnd_cust_rec = cust_abandon_recommendation(final_recommendation)

    # load data into database
    load_db(abnd_cust_rec)
