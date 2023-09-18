from multiprocessing import connection
from tkinter.tix import IMAGE
import pandas as pd
import numpy as np
import os
import json
import streamlit as st
from PIL import Image
import os
import json
from streamlit_option_menu import option_menu
import pandas as pd
import sqlite3
import mysql.connector as mysql
import plotly.express as px
from sqlalchemy import create_engine



path = "C:/Users/Dell/DataScience_Workbench/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list = os.listdir(path)
#Agg_state_list
# Agg_state_list--> to get the list of states in India

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'Transacion_type': [],
       'Transacion_count': [], 'Transacion_amount': []}
for i in Agg_state_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transacion_type'].append(Name)
                    clm['Transacion_count'].append(count)
                    clm['Transacion_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass
Agg_Trans=pd.DataFrame(clm)
Agg_Trans.to_csv('aggregated_transaction',index=False)



path = "C:/Users/Dell/DataScience_Workbench/pulse/data/aggregated/user/country/india/state/"
user_list = os.listdir(path)
#Agg_state_list


# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'Brand': [],
    'Brand_count': [], 'Brand_percentage': []}
for i in user_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["usersByDevice"]:
                    
                    brand = z['brand']

                    brand_count = z['count']
                    brand_percentage = z["percentage"]
                    clm['Brand'].append(brand)
                    clm['Brand_count'].append(brand_count)
                    clm['Brand_percentage'].append(brand_percentage)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass 
                
df_aggregated_user = pd.DataFrame(clm)
df_aggregated_user.to_csv('aggregated_user',index=False)




path = "C:/Users/Dell/DataScience_Workbench/pulse/data/map/transaction/hover/country/india/state/"
hover_list = os.listdir(path)
#Agg_state_list


# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'District': [],
    'Transaction_count': [], 'Transaction_amount': []}
for i in hover_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverDataList"]:
                    district = z['name']
                    transaction_count = z['metric'][0]['count']
                    transaction_amount = z['metric'][0]['amount']
                    clm['District'].append(district)
                    clm['Transaction_count'].append(transaction_count)
                    clm['Transaction_amount'].append(transaction_amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
# Succesfully created a dataframe
            except:
                pass   

df_map_transaction = pd.DataFrame(clm)
df_map_transaction.to_csv('map_transaction',index=False)





path = "C:/Users/Dell/DataScience_Workbench/pulse/data/map/user/hover/country/india/state/"
map_list = os.listdir(path)
#Agg_state_list

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'District': [],
    'Registered_user': [], 'App_opening': []}
for i in map_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverData"]:
                    district = z
                    registered_user =  D['data']["hoverData"][z]["registeredUsers"]
                    app_opening = D['data']["hoverData"][z]["appOpens"]
                    clm['District'].append(district)
                    clm['Registered_user'].append(registered_user)
                    clm['App_opening'].append(app_opening)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
# Succesfully created a dataframe
            except:
                pass       

df_map_user = pd.DataFrame(clm)
df_map_user.to_csv('map_user',index=False)



path = "C:/Users/Dell/DataScience_Workbench/pulse/data/top/transaction/country/india/state/"
TOP_list = os.listdir(path)

clm = {'State': [],  'District': [], 'Year': [], 'Quater': [],'Transaction_count': [],
        'Transaction_amount': []}
for i in TOP_list:
    p_i = path + i + "/"
    year = os.listdir(p_i)

    for j in year:
        p_j = p_i + j + "/"
        file = os.listdir(p_j)

        for k in file:
            p_k = p_j + k
            # print(p_k)
            Data = open(p_k, 'r')
            E = json.load(Data)
            for z in E['data']['pincodes']:
                Name = z['entityName']
                count = z['metric']['count']
                amount = z['metric']['amount']
                clm['District'].append(Name)
                clm['Transaction_count'].append(count)
                clm['Transaction_amount'].append(amount)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quater'].append(int(k.strip('.json')))
  
df_top_transaction = pd.DataFrame(clm)
df_top_transaction.to_csv('top_transaction',index=False)



path = "C:/Users/Dell/DataScience_Workbench/pulse/data/top/user/country/india/state/"
USER_list = os.listdir(path )

clm = {'State': [],  'District': [],'Year': [], 'Quater': [],
        'RegisteredUser': []}
for i in USER_list:
    p_i = path + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            # print(p_k)
            Data = open(p_k, 'r')
            F = json.load(Data)
            for t in F['data']['pincodes']:
                Name = t['name']
                registeredUser = t['registeredUsers']
                clm['District'].append(Name)
                clm['RegisteredUser'].append(registeredUser)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quater'].append(int(k.strip('.json')))

df_top_user = pd.DataFrame(clm)
df_top_user.to_csv('top_user',index=False)


df_aggregated_transaction = pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/aggregated_transaction')


# TO GET THE DATA-FRAME OF AGGREGATED <--> USER
df_aggregated_user=pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/aggregated_user')

# TO GET THE DATA-FRAME OF MAP <--> TRANSACTION
df_map_transaction=pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/map_transaction')

# TO GET THE DATA-FRAME OF MAP <--> USER
df_map_user=pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/map_user')

# TO GET THE DATA-FRAME OF TOP <--> TRANSACTION
df_top_transaction=pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/top_transaction')

# TO GET THE DATA-FRAME OF TOP <--> USER
df_top_user=pd.read_csv('C:/Users/Dell/DataScience_Workbench/pulse/top_user')


# CREATING CONNECTION WITH SQL SERVER
def create_database_connection():
    connection = mysql.connect(host="localhost",
                    user="root",
                    password="root",
                    database="phonepe_db_new",
                    use_pure=True
                    )
    return connection


db_uri = 'mysql://root:root@localhost:3306/phonepe_db_new'
engine = create_engine(db_uri)


# Inserting each Data frame into sql server
df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists='replace')
df_aggregated_user.to_sql('aggregated_user', engine, if_exists='replace')
df_map_transaction.to_sql('map_transaction', engine, if_exists='replace')
df_map_user.to_sql('map_user', engine, if_exists='replace')
df_top_transaction.to_sql('top_transaction', engine, if_exists='replace')
df_top_user.to_sql('top_user', engine, if_exists='replace')


def load_data_from_database(query):
    connection = create_database_connection()
    cursor = connection.cursor(buffered=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# Define queries for each option
queries = {
    "Top 10 states based on year and amount of transaction": "SELECT DISTINCT State, SUM(Transaction_amount) as Total_amount FROM top_transaction GROUP BY State ORDER BY total_amount DESC LIMIT 10",
    "Least 10 states based on type and amount of transaction": "SELECT DISTINCT state, sum(transacion_amount) as transaction_amount FROM aggregated_transaction group by state order by transaction_amount limit 10",
    "Top 10 mobile brands based on percentage of transaction": "SELECT DISTINCT brand,avg(brand_percentage) as brand_percentage FROM aggregated_user group by brand ORDER BY brand_percentage DESC LIMIT 10",
    "Least 10 mobile brands based on percentage of transaction": "SELECT DISTINCT brand,avg(brand_percentage) as brand_percentage FROM aggregated_user group by brand ORDER BY brand_percentage asc LIMIT 10",
    "Top 10 Registered-users based on States and District(pincode)": "SELECT DISTINCT state, district, sum(registeredUser) as registeredUser FROM top_user group by state, district order by registeredUser desc limit 10",
    "Least 10 registered-users based on Districts and states": "SELECT state, district, sum(registeredUser) as registeredUser FROM top_user group by state, district order by registeredUser limit 10",
    "Top 10 Districts based on states and amount of transaction": "SELECT district, state, sum(transaction_amount) as transaction_amount FROM map_transaction group by state, district order by transaction_amount desc limit 10",
    "Least 10 Districts based on states and amount of transaction": "SELECT district, state, sum(transaction_amount) as transaction_amount FROM map_transaction group by state, district order by transaction_amount limit 10",
    "Top 10 transactions_type based on states and transaction_amount": "SELECT state, transacion_type, sum(transacion_amount) as transaction_amount FROM aggregated_transaction group by state, transacion_type order by transaction_amount desc limit 10",
    "Registered Users based on Year and Quater": "SELECT year, quater, sum(transaction_amount) as transaction_amount FROM top_transaction group by year, quater order by transaction_amount desc"
}

def main():
    img=Image.open('PhonePe_Logo.png')
    st.set_page_config(page_title='PhonePe Pulse',page_icon=img,layout='wide')
    image = st.image(img, width=50)

    st.title(':violet[ PhonePe Pulse Data Visualization ]')
    
    
    tab1, tab2,tab3 = st.tabs(["Home", "Analysis","About Pulse"])
    with tab1:
        subheader_text = "Welcome to the PhonePe Pulse Data Visualization, your gateway to a world of data-driven insights and captivating visualizations! Here, we aim to provide you with a comprehensive understanding of PhonePe's digital finance landscape, backed by cutting-edge data analytics and visual representations."
        st.markdown(f"<h2 style='font-size: 22px;'>{subheader_text}</h2>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            st.title("Data Insights")
            st.write("Delve into the heart of PhonePe's transaction data, user behaviors, and trends. We analyze millions of transactions to uncover patterns, preferences, and emerging market trends. Whether you're a curious user or a business enthusiast, our data insights offer valuable knowledge that can inform your decisions.")
        with col2:
            st.title("Visualizations")
            st.write("Our visualizations bring data to life in a user-friendly way. Explore easy-to-understand charts and graphs showing PhonePe's growth and impact. From regional trends to transaction volume, our visuals provide a clear view of digital finance.")

    with tab2:
        st.subheader("Dive into Data Insights and Visualizations!")
        options = ["--select--",
                "Top 10 states based on year and amount of transaction",
                "Least 10 states based on type and amount of transaction",
                "Top 10 mobile brands based on percentage of transaction",
                "Least 10 mobile brands based on percentage of transaction",
                "Top 10 Registered-users based on States and District(pincode)",
                "Least 10 registered-users based on Districts and states",
                "Top 10 Districts based on states and amount of transaction",
                "Least 10 Districts based on states and amount of transaction",
                "Top 10 transactions_type based on states and transaction_amount",
                "Registered Users based on Year and Quater"]# Streamlit app title and description
        st.title("PhonePe Geo Visualization Dashboard")
        st.write("Select options to visualize data")
        select = st.selectbox("Please select the option to get insights",options)
        if select != "--select--":
            query = queries.get(select)
            if select == "Top 10 states based on year and amount of transaction":     
                data = load_data_from_database(query)
                df = pd.DataFrame(data, columns=['State','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Top 10 states based on type and amount of transaction")
                    fig=px.bar(df,x="State",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
                                
            elif select=="Least 10 states based on type and amount of transaction":
                data = load_data_from_database(query) 
                df = pd.DataFrame(data,columns=['State','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Least 10 states based on type and amount of transaction")
                    fig=px.bar(df,x="State",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            elif select=="Top 10 mobile brands based on percentage of transaction":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['brands','Percentage'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Top 10 mobile brands based on percentage of transaction")
                    fig=px.bar(df,x="brands",y="Percentage")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            elif select=="Least 10 mobile brands based on percentage of transaction":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['Brand','Percentage'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Least 10 mobile brands based on percentage of transaction")
                    fig=px.bar(df,x="Brand",y="Percentage")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
                
                
            elif select=="Top 10 Registered-users based on States and District(pincode)":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['State','District','RegisteredUser'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Top 10 Registered-users based on States and District(pincode)")
                    fig=px.bar(df,x="State",y="RegisteredUser")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            elif select=="Least 10 registered-users based on Districts and states":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['State','District','RegisteredUser'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Least 10 registered-users based on Districts and states")
                    fig=px.bar(df,x="State",y="RegisteredUser")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
                    
            elif select=="Top 10 Districts based on states and amount of transaction":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['District','State','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Top 10 Districts based on states and amount of transaction")
                    fig=px.bar(df,x="District",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
                    
            elif select=="Least 10 Districts based on states and amount of transaction":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['District','State','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Least 10 Districts based on states and amount of transaction")
                    fig=px.bar(df,x="District",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
                    
            elif select=="Top 10 transactions_type based on states and transaction_amount":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['State','Transaction_type','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Top 10 transactions_type based on states and transaction_amount")
                    fig=px.bar(df,x="State",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            elif select=="Registered Users based on Year and Quater":
                data = load_data_from_database(query)
                df = pd.DataFrame(data,columns=['Year','Quater','Transaction_amount'])
                col1,col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    st.subheader("Registered Users based on Year and Quater")
                    fig=px.bar(df,x="Year",y="Transaction_amount")
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    with tab3:
        st.write("The Indian digital payments story has truly captured the world's imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones and data. When PhonePe started 5 years back, we were constantly looking for definitive data sources on digital payments in India. Some of the questions we were seeking answers to were - How are consumers truly using digital payments? What are the top cases? Are kiranas across Tier 2 and 3 getting a facelift with the penetration of QR codes?This year as we became Indias largest digital payments platform with 46% UPI market share, we decided to demystify the what, why and how of digital payments in India.This year, as we crossed 2000 Cr. transactions and 30 Crore registered users, we thought as India's largest digital payments platform with 46% UPI market share, we have a ring-side view of how India sends, spends, manages and grows its money. So it was time to demystify and share the what, why and how of digital payments in India.PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team.")
if __name__ == "__main__":
    main()