import pandas as pd
import json
import os
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text
import requests
import matplotlib.pyplot as plt

path = r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\aggregated\transaction\country\india\state"
Agg_trans_list = os.listdir(path)

clm = {
    'State': [], 
    'Years': [], 
    'Quarter': [], 
    'Transaction_type': [], 
    'Transaction_count': [], 
    'Transaction_amount': []
}

for i in Agg_trans_list:
    p_i = os.path.join(path, i)
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = os.path.join(p_i, j)
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)
            if D['data']['transactionData'] is not None:
                for z in D['data']['transactionData']:
                    clm['Transaction_type'].append(z['name'])
                    clm['Transaction_count'].append(z['paymentInstruments'][0]['count'])
                    clm['Transaction_amount'].append(z['paymentInstruments'][0]['amount'])
                    clm['State'].append(i)
                    clm['Years'].append(int(j))
                    clm['Quarter'].append(int(k.strip('.json')))

# Create DataFrame
Agg_Trans = pd.DataFrame(clm)
print(f" Extracted rows: {len(Agg_Trans)}")

path_2=r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\aggregated\user\country\india\state"
aggre_user_list = os.listdir(path_2)

column_2 = {"States":[], "Years":[], "Quarter":[], "Brands":[], "Transaction_count":[], "Percentage":[]}

for state in aggre_user_list:
    present_states = os.path.join(path_2, state)
    aggre_year_list = os.listdir(present_states)

    for year in aggre_year_list:
        present_year = os.path.join(present_states,year)
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = os.path.join(present_year,file)
            data = open(present_file, "r")
            U = json.load(data)

            try:

                for i in U["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count =i["count"]
                    percentage = i["percentage"]
                    column_2["Brands"].append(brand)
                    column_2["Transaction_count"].append(count)
                    column_2["Percentage"].append(percentage)
                    column_2["States"].append(state)
                    column_2["Years"].append(year)
                    column_2["Quarter"].append(int(file.strip(".json")))
            except:
                pass

aggre_user=pd.DataFrame(column_2)

aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user["States"] = aggre_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")





path_3=r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\map\transaction\hover\country\india\state"
map_trans_list = os.listdir(path_3)

column_3 = {"States":[], "Years":[], "Quarter":[], "District":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_trans_list:
    present_states = os.path.join(path_3,state)
    map_year_list = os.listdir(present_states)

    for year in map_year_list:
        present_year = os.path.join(present_states,year)
        map_file_list = os.listdir(present_year)

        for file in map_file_list:
            present_file = os.path.join(present_year,file)
            data = open(present_file, "r")
            D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                column_3["District"].append(name)
                column_3["Transaction_count"].append(count)
                column_3["Transaction_amount"].append(amount)
                column_3["States"].append(state)
                column_3["Years"].append(year)
                column_3["Quarter"].append(int(file.strip(".json")))

map_transaction=pd.DataFrame(column_3)

map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction["States"] = map_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(map_transaction)}")



path_4=r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\map\user\hover\country\india\state"
map_user_list = os.listdir(path_4)

column_4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user_list:
    present_states = os.path.join(path_4,state)
    map_year_list = os.listdir(present_states)

    for year in map_year_list:
        present_year = os.path.join(present_states,year)
        map_file_list = os.listdir(present_year)

        for file in map_file_list:
            present_file=os.path.join(present_year,file)
            data=open(present_file, "r")
            H = json.load(data)

            for i in H["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                column_4["Districts"].append(district)
                column_4["RegisteredUser"].append(registereduser)
                column_4["AppOpens"].append(appopens)
                column_4["States"].append(state)
                column_4["Years"].append(year)
                column_4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(column_4)

map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user["States"] = map_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(map_user)}")



path_5=r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\top\transaction\country\india\state"
top_trans_list = os.listdir(path_5)

column_5 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_trans_list:
    present_states = os.path.join(path_5,state)
    top_trans_list = os.listdir(present_states)

    for year in top_trans_list:
        present_year = os.path.join(present_states,year)
        top_file_list = os.listdir(present_year)

        for file in top_file_list:
            present_file = os.path.join(present_year,file)
            data = open(present_file, "r")
            A = json.load(data)

            for i in A["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                column_5["Pincodes"].append(entityName)
                column_5["Transaction_count"].append(count)
                column_5["Transaction_amount"].append(amount)
                column_5["States"].append(state)
                column_5["Years"].append(year)
                column_5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(column_5)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction["States"] = top_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(top_transaction)}")



path_6 = r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\top\user\country\india\state"
top_user_list = os.listdir(path_6)

column_6 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_list:
    present_states = os.path.join(path_6,state)
    top_year_list = os.listdir(present_states)

    for year in top_year_list:
        present_year = os.path.join(present_states,year)
        top_file_list = os.listdir(present_year)

        for file in top_file_list:
            present_file = os.path.join(present_year,file)
            data = open(present_file,"r")
            K = json.load(data)

            for i in K["data"]["pincodes"]:
                name = i["name"]
                registereduser = i["registeredUsers"]
                column_6["Pincodes"].append(name)
                column_6["RegisteredUser"].append(registereduser)
                column_6["States"].append(state)
                column_6["Years"].append(year)
                column_6["Quarter"].append(int(file.strip(".json")))


top_user = pd.DataFrame(column_6)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user["States"] = top_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")
print(f" Extracted rows: {len(top_user)}")


path_7 =r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\aggregated\insurance\country\india\state"
# Initialize column structure
column_aggre_insurance = {
    "States": [], "Years": [], "Quarter": [],
    "Insurance_type": [], "Total_count": [], "Total_amount": []
}

# Get list of state folders
aggre_insurance_list = os.listdir(path_7)

for state_raw in aggre_insurance_list:
    state_path = os.path.join(path_7, state_raw)
    if not os.path.isdir(state_path):
        continue  # Skip if not a directory

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    year_list = os.listdir(state_path)

    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue  # Skip if not a directory

        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                # Safely extract insurance transaction data
                transaction_data = data.get("data", {}).get("transactionData", [])
                for txn in transaction_data:
                    if txn.get("name") == "Insurance":
                        instruments = txn.get("paymentInstruments", [])
                        if isinstance(instruments, list) and len(instruments) > 0:
                            count = instruments[0].get("count", 0)
                            amount = instruments[0].get("amount", 0)

                            column_aggre_insurance["States"].append(state)
                            column_aggre_insurance["Years"].append(int(year))
                            column_aggre_insurance["Quarter"].append(int(file.replace(".json", "")))
                            column_aggre_insurance["Insurance_type"].append("Total")
                            column_aggre_insurance["Total_count"].append(count)
                            column_aggre_insurance["Total_amount"].append(amount)

            except Exception as e:
                print(f" Error processing file: {file_path} — {e}")

#  Convert dict → DataFrame
aggre_insurance = pd.DataFrame(column_aggre_insurance)

print(f" Extracted rows: {len(aggre_insurance)}")



path_8 = r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\map\insurance\hover\country\india\state"

# Initialize structure
column_map_insurance = {
    "States": [], "Districts": [], "Years": [], "Quarter": [],
    "Insurance_Category": [], "Transaction_count": [], "Transaction_amount": []
}

# Loop through states
map_insurance_list = os.listdir(path_8)

for state_raw in map_insurance_list:
    state_path = os.path.join(path_8, state_raw)
    if not os.path.isdir(state_path):
        continue

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    # Loop through years
    year_list = os.listdir(state_path)
    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        # Loop through quarters (files)
        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    MI = json.load(f)

                hover_data_list = MI.get("data", {}).get("hoverDataList", [])

                for item in hover_data_list:
                    district = item.get("name")
                    metric = item.get("metric")

                    if isinstance(metric, list) and len(metric) > 0:
                        count = metric[0].get("count", 0)
                        amount = metric[0].get("amount", 0)

                        column_map_insurance["Districts"].append(district)
                        column_map_insurance["Insurance_Category"].append("TOTAL")
                        column_map_insurance["Transaction_count"].append(count)
                        column_map_insurance["Transaction_amount"].append(amount)
                        column_map_insurance["States"].append(state)
                        column_map_insurance["Years"].append(int(year))
                        column_map_insurance["Quarter"].append(int(file.replace(".json", "")))

            except Exception as e:
                print(f" Error processing {file_path}: {e}")

#  Convert dict → DataFrame
map_insurance = pd.DataFrame(column_map_insurance)

# Check extracted data
print(f" Extracted rows: {len(map_insurance)}")



path_9 = r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\top\insurance\country\india\state"

# Initialize column structure
column_top_insurance = {
    "States": [], "Years": [], "Quarter": [], "Pincodes": [],
    "Insurance_Category": [], "Transaction_count": [], "Transaction_amount": []
}

# Loop through states
top_insurance_list = os.listdir(path_9)

for state_raw in top_insurance_list:
    state_path = os.path.join(path_9, state_raw)
    if not os.path.isdir(state_path):
        continue

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    # Loop through years
    year_list = os.listdir(state_path)
    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        # Loop through quarters (files)
        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    TI = json.load(f)

                # Extract Pincodes-level data
                pincode_data = TI.get("data", {}).get("pincodes", [])
                for item in pincode_data:
                    pincode = item.get("entityName")
                    metric = item.get("metric", {})

                    count = metric.get("count", 0)
                    amount = metric.get("amount", 0)

                    column_top_insurance["Pincodes"].append(pincode)
                    column_top_insurance["Insurance_Category"].append("TOTAL")  # Always total
                    column_top_insurance["Transaction_count"].append(count)
                    column_top_insurance["Transaction_amount"].append(amount)
                    column_top_insurance["States"].append(state)
                    column_top_insurance["Years"].append(int(year))
                    column_top_insurance["Quarter"].append(int(file.replace(".json", "")))

            except Exception as e:
                print(f" Error processing {file_path}: {e}")

#  Convert to DataFrame
top_insurance = pd.DataFrame(column_top_insurance)

# Check extracted data
print(f" Extracted rows: {len(top_insurance)}")





engine = create_engine("mysql+mysqlconnector://Phonepe:Phonepe@127.0.0.1:3306/Phonepe")

Agg_Trans.to_sql("transactions1", con=engine, if_exists="replace", index=False)
aggre_user.to_sql("transactions2", con=engine, if_exists="replace", index=False)
map_transaction.to_sql("transactions3", con=engine, if_exists="replace", index=False)
map_user.to_sql("transactions4", con=engine, if_exists="replace", index=False)
top_transaction.to_sql("transactions5", con=engine, if_exists="replace", index=False)
top_user.to_sql("transactions6", con=engine, if_exists="replace", index=False)
aggre_insurance.to_sql("transactions7", con=engine, if_exists="replace", index=False)
map_insurance.to_sql("transactions8", con=engine, if_exists="replace", index=False)
top_insurance.to_sql("transactions9", con=engine, if_exists="replace", index=False)













def run_query(query):
    with engine.connect() as con:
        return pd.read_sql(query, con=engine)

# ---------------------------
# Page Config & Styling
# ---------------------------
st.set_page_config(page_title="PhonePe Pulse Insights", layout="wide")

st.markdown("""
    <style>
        .main {background-color: #f8f6ff; padding: 20px; border-radius: 12px;}
        h1,h2,h3 {color: #5a2ca0;}
        .sidebar .sidebar-content {background-color: #e6e0f8;}
    </style>
""", unsafe_allow_html=True)

# ---------------------------
# Sidebar Navigation
# ---------------------------
st.sidebar.title("📊 PhonePe Dashboard")
page = st.sidebar.selectbox(
    "Navigate",
    ["Home", "Transactions", "Insurance", "User Registrations", 
     "Engagement & Growth", "Device Dominance"]
)

# ---------------------------
# Filters
# ---------------------------
years = run_query("SELECT DISTINCT Years FROM transactions1 ORDER BY Years")["Years"].tolist()
quarters = run_query("SELECT DISTINCT Quarter FROM transactions1 ORDER BY Quarter")["Quarter"].tolist()

year_filter = st.sidebar.selectbox("Year", years, index=len(years)-1)
quarter_filter = st.sidebar.selectbox("Quarter", quarters)

# ---------------------------
# Load India GeoJSON
# ---------------------------
st.set_page_config(page_title="PhonePe Dashboard", layout="wide")

# ---------------------------
# Home (India Map)
# ---------------------------
if page == "Home":
    st.markdown("<h1>PhonePe Pulse Insights</h1>", unsafe_allow_html=True)

    #df = pd.read_csv("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/active_cases_2020-07-17_0800.csv")
    df = run_query(f"""
        SELECT State, SUM(Transaction_amount) AS TotalAmount
        FROM transactions1
        WHERE Years={year_filter} AND Quarter={quarter_filter}
        GROUP BY State
    """)
    
    state_mapping = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra and Nagar Haveli and Daman and Diu",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"
    }
    df=df.replace(state_mapping)
    
#Create the choropleth map
    fig = px.choropleth(
    df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='TotalAmount',
    color_continuous_scale='Reds'
    )

    fig.update_geos(fitbounds="locations", visible=True)
    st.plotly_chart(fig)


    st.dataframe(df)


    
# ---------------------------
# Transactions
# ---------------------------
elif page == "Transactions":
    st.markdown("<h2>Transaction Dynamics</h2>", unsafe_allow_html=True)

    df = run_query(f"""
        SELECT Years, Quarter, Transaction_type,
       SUM(Transaction_count) AS TxnCount,
       SUM(Transaction_amount) AS TxnAmount
       FROM transactions1
       WHERE Years={year_filter} AND Quarter={quarter_filter}
       GROUP BY Years, Quarter, Transaction_type
    """)
    df1 = run_query(f"""
        SELECT 
        Years, 
        Quarter, 
        Transaction_type, 
        SUM(Transaction_count) AS Total_Count,
        SUM(Transaction_amount) AS Total_Amount
        FROM transactions1
        GROUP BY Years, Quarter, Transaction_type
        ORDER BY Years, Quarter""")

    # Line chart
    fig_line = px.line(df, x="Transaction_type", y="TxnAmount", markers=True,
                       title="Transaction Amount by Type")
    st.plotly_chart(fig_line, use_container_width=True)

    # Bar chart
    fig_bar = px.bar(df, x="Transaction_type", y="TxnCount", color="Transaction_type",
                     title="Transaction Count by Type")
    st.plotly_chart(fig_bar, use_container_width=True)

    

    # Create Year_Quarter column
    df1["Year_Quarter"] = df1["Years"].astype(str) + "-Q" + df1["Quarter"].astype(str)

    # Pivot tables
    pivot_counts = df1.pivot_table(
        index="Year_Quarter", columns="Transaction_type",
        values="Total_Count", aggfunc="sum", fill_value=0
    )

    pivot_amounts = df1.pivot_table(
        index="Year_Quarter", columns="Transaction_type",
        values="Total_Amount", aggfunc="sum", fill_value=0
    )

    # Check if they are empty before plotting
    print("Pivot Counts shape:", pivot_counts.shape)
    print("Pivot Amounts shape:", pivot_amounts.shape)
    print(pivot_counts.head())
    print(pivot_amounts.head())

    if not pivot_counts.empty and not pivot_amounts.empty:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))

        # Plot counts
        pivot_counts.plot(kind="bar", stacked=True, ax=ax1)
        ax1.set_title("Transaction Count by Type across Years and Quarters")
        ax1.set_xlabel("Year - Quarter")
        ax1.set_ylabel("Transaction Count")
        ax1.legend(title="Transaction Type", bbox_to_anchor=(1.05, 1))

        # Plot amounts
        pivot_amounts.plot(kind="bar", stacked=True, ax=ax2)
        ax2.set_title("Transaction Amount by Type across Years and Quarters")
        ax2.set_xlabel("Year - Quarter")
        ax2.set_ylabel("Transaction Amount (INR)")
        ax2.legend(title="Transaction Type", bbox_to_anchor=(1.05, 1))

        plt.tight_layout()
        st.pyplot(fig)
    else:
        print("⚠️ Pivot table is empty. Check column names and data.")



# ---------------------------
# Insurance
# ---------------------------
elif page == "Insurance":
    st.markdown("<h2>Insurance Transactions</h2>", unsafe_allow_html=True)

    df = run_query(f"""
        SELECT States,
       SUM(Total_count) AS TxnCount,
       SUM(Total_amount) AS TxnAmount
       FROM transactions7
       WHERE Years={year_filter} AND Quarter={quarter_filter}
       GROUP BY States

    """)

    fig = px.bar(df, x="States", y="TxnAmount", color="TxnCount",
                 title="Insurance Amount & Count by State")
    st.plotly_chart(fig, use_container_width=True)

    df1 = run_query(f"""
        SELECT Districts,
       SUM(Transaction_count) AS TxnCount,
       SUM(Transaction_amount) AS TxnAmount
       FROM transactions8
       WHERE Years={year_filter} AND Quarter={quarter_filter}
       GROUP BY Districts

    """)

    fig1 = px.bar(df1, x="Districts", y="TxnAmount", color="TxnCount",
                 title="Insurance Amount & Count by Districts")
    st.plotly_chart(fig1, use_container_width=True)

# ---------------------------
# User Registrations
# ---------------------------
elif page == "User Registrations":
    st.markdown("<h2>User Registrations</h2>", unsafe_allow_html=True)

    df = run_query(f"""
        SELECT States, SUM(RegisteredUser) AS Users
        FROM transactions6
        WHERE Years={year_filter} AND Quarter={quarter_filter}
        GROUP BY States
        ORDER BY Users DESC
    """)

    fig = px.bar(df, x="Users", y="States", orientation="h",
                 title="Top States by User Registrations")
    st.plotly_chart(fig, use_container_width=True)

    df1 = run_query(f"""
       SELECT Districts, SUM(RegisteredUser) AS Total_Users
        FROM transactions4
        WHERE Years = {year_filter}  AND Quarter = {quarter_filter}
        GROUP BY Districts
        ORDER BY Total_Users DESC
    """)

    fig1 = px.line(df1, x="Districts", y="Total_Users", orientation="h",
                 title="Top Districts by User Registrations")
    st.plotly_chart(fig1, use_container_width=True)
    
    

# ---------------------------
# Engagement & Growth
# ---------------------------
elif page == "Engagement & Growth":
    st.markdown("<h2>Engagement & Growth</h2>", unsafe_allow_html=True)

    df = run_query(f"""
        SELECT States, SUM(RegisteredUser) AS Users, SUM(AppOpens) AS Opens
        FROM transactions4
        WHERE Years={year_filter} AND Quarter={quarter_filter}
      GROUP BY States

    """)

    fig = px.scatter(df, x="Users", y="Opens", color="States", size="Users",
                     title="User Engagement (App Opens vs Registrations)")
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------
# Device Dominance
# ---------------------------
elif page == "Device Dominance":
    st.markdown("<h2>Device Dominance</h2>", unsafe_allow_html=True)

    df = run_query(f"""
        SELECT Brands, SUM(Transaction_count) AS Users
        FROM transactions2
        WHERE Years={year_filter} AND Quarter={quarter_filter}
        GROUP BY Brands

    """)

    fig_pie = px.pie(df, values="Users", names="Brands", hole=0.4,
                     title="Market Share by Device Brands")
    st.plotly_chart(fig_pie, use_container_width=True)

    fig_line = px.line(df, x="Brands", y="Users", markers=True,
                       title="Device-wise User Distribution")
    st.plotly_chart(fig_line, use_container_width=True)




