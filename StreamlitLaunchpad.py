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

    pref_size = st.multiselect("Enter preferred sizes",Scrapper.get_all_existing_sizes(st.session_state["complete_df"]))
    #
    pref_available = st.checkbox("Show only available")
    if pref_size or pref_name or pref_stores or pref_available:
        st.session_state["filtered_df"] = st.session_state["complete_df"]


        st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.contains(pref_name, na=False)]

        if pref_stores:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["store"].isin(pref_stores)]

        if pref_size:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["size"].isin(pref_size)]

        st.session_state["filtered_df"] = st.session_state["filtered_df"].query("available == 'True'")


    else:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

with col2:
    st.dataframe(st.session_state["filtered_df"])
    st.text(f"Amount of filtered records: {len(st.session_state["filtered_df"])}")
    st.text(f"Amount of total records: {len(st.session_state["complete_df"])}")


def time_format(start_time) -> float:
    """
    Method for calculating time difference
    :param start_time: start time of the scrap
    :return: time (float) scrap took
    """
    global DATA_PULL_TOTAL_TIME
    dif = time.time()-start_time
    DATA_PULL_TOTAL_TIME += dif
    return round(dif,2)

def scrap_complete_data(list_of_stores:list=None):
    global DATA_PULL_TOTAL_TIME
    DATA_PULL_TOTAL_TIME=0
    start = time.time()
    complete_data = []
    excluded_stores = [x for x,check in zip(Scrapper.STORES_SCRAPPERS.keys(),list_of_stores) if not check]

    #security check
    if "passok" not in st.session_state.keys() or not st.session_state["passok"]:
        st.toast("Provide proper password before running the scrapping")
    #todo check threaded method!!!
    thread_list = []
    tmp_store_states = {key:"?" for key in Scrapper.STORES_SCRAPPERS.keys()}
    for store_name, store_scrap in Scrapper.STORES_SCRAPPERS.items():
        if store_name in excluded_stores:
            continue
        def pull_single_store(store_name_arg):
            start_time = time.time()
            try:

                res = store_scrap()
                complete_data.extend(res)
                #st.session_state["pulled_data"][store_name_arg] = res
                if not res:
                #     print(f"EMPTY STORE: {store_name_arg}")
                #     msg = st.toast(f"ERROR - Failed to scrap {store_name_arg} data ({time_format(start_time)}s)")
                #     #st.session_state["loaded_stores"][store_name_arg] = "ERROR"
                    tmp_store_states[store_name_arg]= "ERROR"
                else:
                #     msg = st.toast(f"OK - Successfully scrapped {store_name_arg} data({time_format(start_time)}s)")
                #     #st.session_state["loaded_stores"][store_name_arg] = "OK"
                    tmp_store_states[store_name_arg]= f"OK ({(time.time()-start_time,2)}s)"
            except Exception as e:
                print(e)
                print(traceback.print_exc())
                msg = st.toast(f"ERROR - Failed to scrap {store_name_arg} data({round(time_format(start_time))}s)")
                st.session_state["loaded_stores"][store_name_arg] = "ERROR"

        thread_list.append(Thread(target=pull_single_store,args=[store_name]))
        thread_list[-1].start()
        #pull_single_store(store_name)

    while any([t.is_alive() for t in thread_list]):
        time.sleep(1)

    #DATA_PULL_TOTAL_TIME = Scrapper.
    #st.session_state["complete_data"]
    st.session_state["loaded_stores"] = tmp_store_states
    st.success(f"Complete scraping took {round(time.time()-start,2)}s")
    try:

        total_df = Scrapper.map_sizes(pd.DataFrame(complete_data))
        total_df = Scrapper.map_prices(total_df)
        total_df["price"] = total_df["price"].fillna('-1').apply(lambda x:'-1' if x=='' else x).astype(float)

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

s_cols = st.columns(len(Scrapper.STORES_SCRAPPERS)+1)
if "stores_checkboxes" not in st.session_state.keys():
    st.session_state["stores_checkboxes"]={store:True for store in Scrapper.STORES_SCRAPPERS.keys()}
choosen_stores = {}
checkboxes = []
for key,val in Scrapper.STORES_SCRAPPERS.items():
    with s_cols[len(checkboxes)]:
        x = st.checkbox(key,value=st.session_state["stores_checkboxes"][key])

        checkboxes.append(x)


def select_all():
    if all(st.session_state["stores_checkboxes"].values()):
        st.session_state["stores_checkboxes"] = {store:False for store in Scrapper.STORES_SCRAPPERS.keys()}

    else:
        for k,v in st.session_state["stores_checkboxes"].items():
            st.session_state["stores_checkboxes"][k]=True

with s_cols[-1]:
    st.button("Select all",on_click=select_all)


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


