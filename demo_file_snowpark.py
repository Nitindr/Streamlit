# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd

conn = st.connection("snowflake")

def pdf():
    sql = "select DS,AGGREGATED_VALUE from DATA_QUALITY_DB.DQ_ADW.DQ_METRIC_SNAPSHOT where metric='arr_account' and grain='monthly' order by ds desc"
    res = conn.query(sql)
    return res

def main():
    st.title(" Welcome to see the line graph")
    res1 = pdf()
    df = pd.DataFrame(res1)

    # Display the data in a Streamlit line chart
    st.line_chart(df.set_index('DS'))

main()



