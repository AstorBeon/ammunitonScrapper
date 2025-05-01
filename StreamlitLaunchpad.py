#Main class to manage rest of the code
import time
import traceback
from threading import Thread

import streamlit as st
import pandas as pd
import Scrapper

st.set_page_config(layout="wide")

# Title
st.title("Find ammo in Warsaw!")
st.subheader("Nifty scrapper collecting data about ammo prices in Warsaw")

title_alignment="""
<style>
h1,h3{
  text-align: center
}
</style>
"""
st.markdown(title_alignment, unsafe_allow_html=True)

#col1,col2 = st.columns([1,3])
if "pulled_data" not in st.session_state.keys():
    st.session_state["pulled_data"]={}
if "complete_df" not in st.session_state.keys():
    st.session_state["complete_df"]=pd.DataFrame()
if "filtered_df" not in st.session_state.keys():
    st.session_state["filtered_df"]=pd.DataFrame()
if "loaded_stores" not in st.session_state.keys():
    st.session_state["loaded_stores"]={x:"?" for x in Scrapper.STORES_SCRAPPERS}
COMPLETE_DATA=pd.DataFrame()
LOADED_STORES = []
#datatable =  st.dataframe(st.session_state["complete_df"])

try:
    st.subheader("Available pages for scrapping")
    for store,status in st.session_state["loaded_stores"].items():
        st.text(f"{store} - {status}")
except Exception as e:
    print(e)

st.markdown("\n")
st.markdown("\n")
st.markdown("\n")

col1,col2 = st.columns([1,3])
with col1:

    pref_name = st.text_input("Enter complete/partial name")

    # if pref_name:
    #     st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.contains(pref_name, na=False)]
    #     #print(f"New length : {len(st.session_state['filtered_df'])}")
    #
    pref_stores = st.multiselect("Preferred stores",list(Scrapper.STORES_SCRAPPERS.keys()))
    #
    # if pref_stores:
    #
    #     st.session_state["filtered_df"] = st.session_state["filtered_df"][
    #         st.session_state["filtered_df"]["store"].isin(pref_stores)]
    #
    pref_size = st.text_input("Enter preferred size")
    #
    # if pref_size:
    #     st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["size"].str.contains(pref_size, na=False)]

    if pref_size or pref_name or pref_stores:
        st.session_state["filtered_df"] = st.session_state["complete_df"]
        print(f"Filtering for pref name: {pref_name}")
        st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.contains(pref_name, na=False)]
        print(st.session_state["filtered_df"])
        print(st.session_state["filtered_df"]["title"].to_list())

        st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["store"].isin(pref_stores)]

        st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["size"].str.contains(pref_size, na=False)]
        print(st.session_state["filtered_df"])
        print(st.session_state["filtered_df"]["size"].to_list())
    else:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

with col2:
    st.dataframe(st.session_state["filtered_df"])
    st.text(f"Amount of filtered records: {len(st.session_state["filtered_df"])}")
    st.text(f"Amount of total records: {len(st.session_state["complete_df"])}")



def scrap_complete_data():
    complete_data = []

    for store_name, store_scrap in Scrapper.STORES_SCRAPPERS.items():
        try:
            res = list(set(store_scrap()))
            complete_data.extend(res)
            st.session_state["pulled_data"][store_name] = res
            if not res:
                msg = st.error(f"Failed to scrap {store_name} data")
                st.session_state["loaded_stores"][store_name] = "ERROR"
            else:
                msg = st.success(f"Successfully scrapped {store_name} data")
                st.session_state["loaded_stores"][store_name] = "OK"
        except Exception as e:
            msg = st.error(f"Failed to scrap {store_name} data")
            st.session_state["loaded_stores"][store_name] = "ERROR"


    try:

        total_df = pd.DataFrame(complete_data)

        st.session_state["complete_df"] = total_df.astype(str)
        st.session_state["filtered_df"] = total_df.astype(str)
        global COMPLETE_DATA
        #COMPLETE_DATA = total_df.astype(str)


        # print("build")
        # #print(total_df.describe())
        # print("------")
        # print(total_df.dtypes)
        #
        # total_df = total_df.astype(str)
        # print(total_df.dtypes)
        # #total_df.to_excel(r"C:\Users\macie\Downloads\complete_data.xlsx")
        # #with col2:
        #     #pass
        # print("add df")
        #st.dataframe(COMPLETE_DATA)


    except Exception as e:
        print(f"Failed to build data: {e}")
        print(traceback.print_exc())


#if not st.session_state["complete_df"] is None:

            # def disappear(prompt):
            #     time.sleep(3)
            #     print("DISAPPEARING")
            #     prompt.empty()
            #
            # Thread(target=disappear,args=[msg]).start()

if not st.session_state["pulled_data"]:
    st.button("Pull current data", on_click=scrap_complete_data,use_container_width=True)









