#Main class to manage rest of the code
import time
import traceback
from threading import Thread
#from streamlit_server_state import server_state, server_state_lock
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
DATA_PULL_TOTAL_TIME=0
#datatable =  st.dataframe(st.session_state["complete_df"])

try:
    st.subheader("Available pages for scrapping")
    cols = st.columns(len(st.session_state["loaded_stores"]))
    count=0
    for store,status in st.session_state["loaded_stores"].items():
        with cols[count]:
            st.text(f"{store} - {status}")
            count+=1
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
    pref_size = st.multiselect("Enter preferred sizes",Scrapper.get_all_existing_sizes(st.session_state["complete_df"]))
    #
    pref_available = st.checkbox("Show only available")
    # if pref_size:
    #     st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["size"].str.contains(pref_size, na=False)]

    if pref_size or pref_name or pref_stores or pref_available:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

        st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.contains(pref_name, na=False)]

        if pref_stores:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["store"].isin(pref_stores)]

        if pref_size:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["size"].isin(pref_size)]

        #st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["size"].str.contains(pref_size, na=False)]

        #print(st.session_state["filtered_df"].query("available == 'True'"))
        st.session_state["filtered_df"] = st.session_state["filtered_df"].query("available == 'True'")


    else:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

with col2:
    st.dataframe(st.session_state["filtered_df"])
    st.text(f"Amount of filtered records: {len(st.session_state["filtered_df"])}")
    st.text(f"Amount of total records: {len(st.session_state["complete_df"])}")


def time_format(start_time):
    global DATA_PULL_TOTAL_TIME
    dif = time.time()-start_time
    DATA_PULL_TOTAL_TIME += dif
    return round(dif,2)

def scrap_complete_data(list_of_stores:list=None):
    global DATA_PULL_TOTAL_TIME
    DATA_PULL_TOTAL_TIME=0
    complete_data = []
    excluded_stores = [x for x,check in zip(Scrapper.STORES_SCRAPPERS.keys(),list_of_stores) if not check]
    #print(excluded_stores)

    #security check
    if "passok" not in st.session_state.keys() or not st.session_state["passok"]:
        st.toast("Provide proper password before running the scrapping")

    thread_list = []
    for store_name, store_scrap in Scrapper.STORES_SCRAPPERS.items():
        if store_name in excluded_stores:
            continue
        def pull_single_store(store_name_arg):
            start_time = time.time()
            try:

                res = store_scrap()
                complete_data.extend(res)
                st.session_state["pulled_data"][store_name_arg] = res
                if not res:
                    print(f"EMPTY STORE: {store_name_arg}")
                    msg = st.toast(f"ERROR - Failed to scrap {store_name_arg} data ({time_format(start_time)}s)")
                    st.session_state["loaded_stores"][store_name_arg] = "ERROR"
                else:
                    msg = st.toast(f"OK - Successfully scrapped {store_name_arg} data({time_format(start_time)}s)")
                    st.session_state["loaded_stores"][store_name_arg] = "OK"
            except Exception as e:
                print(e)
                print(traceback.print_exc())
                msg = st.toast(f"ERROR - Failed to scrap {store_name_arg} data({time_format(start_time)}s)")
                st.session_state["loaded_stores"][store_name_arg] = "ERROR"

        pull_single_store(store_name)

    st.success(f"Complete scraping took {round(DATA_PULL_TOTAL_TIME,2)}s")
    try:

        total_df = Scrapper.map_sizes(pd.DataFrame(complete_data))
        total_df = Scrapper.map_prices(total_df)
        options = Scrapper.get_all_existing_sizes(total_df)


        st.session_state["complete_df"] = total_df.astype(str)
        st.session_state["filtered_df"] = total_df.astype(str)
        global COMPLETE_DATA


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

#if not st.session_state["pulled_data"]:
st.markdown("\n")

s_cols = st.columns(len(Scrapper.STORES_SCRAPPERS))
choosen_stores = {}
checkboxes = []
for key,val in Scrapper.STORES_SCRAPPERS.items():
    with s_cols[len(checkboxes)]:
        x = st.checkbox(key,value=True)
        checkboxes.append(x)


@st.dialog("Quick instruction")
def quick_instruction():

    st.write(f"To get the data, select stores in the bottom of the page and press \"Pull data\" button.")



@st.dialog("Provide pass")
def ask_for_password():

    st.write(f"What's the password?")
    reason = st.text_input("Password:")

    if st.button("Submit"):
        st.session_state["passok"] = reason=="gunlobby"

        st.rerun()



if "passok" in st.session_state.keys() and st.session_state["passok"]:

    st.button("Pull current data", on_click=scrap_complete_data,args=[checkboxes],use_container_width=True)
else:
    ask_for_password()

if not "manual_read" in st.session_state.keys() or not st.session_state["manual_read"]:
    st.session_state["manual_read"] = True
    quick_instruction()

# with server_state_lock["count"]:  # Lock the "count" state for thread-safety
#     if "count" not in server_state:
#         server_state.count = 0
#     server_state["count"] += 1
#     st.write("Count = ", server_state.count)
#
#
#
#
#
#
#
# st.write("Count = ", server_state.count)
#


