import streamlit as st
import pandas as pd

conn = st.connection("snowflake")

# Query Snowflake to get list of databases
def get_databases():
    ##db_query = f"SELECT DISTINCT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES ORDER BY 1;"
    db_query = f"SELECT DISTINCT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES where upper(database_name)='ADW_DB' ORDER BY 1;"
    db = conn.query(db_query)
    return db.iloc[0,0]

# Query Snowflake to get list of schemas
def get_schemas(database):
    ##sch_query = f"SELECT DISTINCT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA ORDER BY 1;"
    sch_query = f"SELECT DISTINCT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA where upper(schema_name)='DS_SALES' ORDER BY 1;"
    sch = conn.query(sch_query)
    return sch.iloc[0,0]

# Query Snowflake to get list of tables,views named as objects
def get_objects(database,schema):
    ##obj_query = f"SELECT DISTINCT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=UPPER('{schema}') ORDER BY 1;"
    obj_query = f"SELECT DISTINCT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=UPPER('{schema}') and upper(table_name) in ('DS_F_ACV','DS_D_PIPEGEN') ORDER BY 1;"
    obj = conn.query(obj_query)
    return obj

# Query Snowflake to get list of ds snapshot dates
def get_ds_dates(database,schema,object):
    ##obj_query = f"SELECT DISTINCT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=UPPER('{schema}') ORDER BY 1;"
    ds_query = f"SELECT DISTINCT DS FROM {database}.{schema}.{object} ORDER BY 1 DESC;"
    ds = conn.query(ds_query)
    return ds 

# Query Snowflake to get the data from object
def get_data(database,schema,object,ds):
    data_query = f"SELECT * FROM {database}.{schema}.{object} WHERE DS='{ds}';"
    data = conn.query(data_query)
    return data 

# main function
def main():
    st.title(" DataTeam - Snowflake Table Explorer ")
    
    # Select the database
    selected_database = get_databases()

    selected_schema = get_schemas(selected_database)

    selected_table = st.selectbox(" Select Object ", get_objects(selected_database, selected_schema),index=None,placeholder="Select Required Object...")
    
    if selected_table is not None:

        selected_ds = st.selectbox(" Select Snapshot Date ", get_ds_dates(selected_database, selected_schema, selected_table),index=None,placeholder="Select Snapshot Dates...")
        
        if selected_ds is not None:
            # Query the table
            df = get_data(selected_database, selected_schema,selected_table,selected_ds)

            #pandas dataframe
            pd_df = pd.DataFrame(df)
            # Display the table
            op=st.dataframe(df)
       
            st.download_button("Download CSV", pd_df.to_csv() , file_name='output_table.csv', mime='text/csv')

##if __name__ == "__main__":
main()