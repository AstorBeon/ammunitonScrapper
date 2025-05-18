#Main class to manage rest of the code
import os
import time
import traceback
from csv import excel
from datetime import datetime, timedelta
from threading import Thread
#from streamlit_server_state import server_state, server_state_lock
import streamlit as st
import pandas as pd
import Scrapper

st.set_page_config(layout="wide")


cities_per_region = {"Mazowieckie":["Warsaw","Płock","Siedlce","Ostrołęka","Ciechanów"],
                     "Łódzkie":["Łódź","Piotrków Trybunalski","Pabianice","Aleksandrów Łódzki"],
                     "Wielkopolskie": ["Poznań"]}



if "date_of_last_pull" not in st.session_state.keys():
    st.session_state["date_of_last_pull"] = "None"

def check_if_last_load_was_at_least_x_minutes_ago(minutes:int):
    try:
        if (datetime.now().timestamp() - os.path.getmtime("my_silly_database.xlsx")+ timedelta(hours=2).total_seconds())/60 < minutes:
            st.toast(f"You can't refresh data more frequently than once per {minutes} minutes")
            return False
    except Exception as e:
        #print(e)
        return True
    return True


def scrap_complete_data(list_of_stores:list=None):
    if not check_if_last_load_was_at_least_x_minutes_ago(minutes=15):
       return

    st.toast("Data pull started. It may take up to 25 seconds. Sit tight :)")
    global DATA_PULL_TOTAL_TIME
    DATA_PULL_TOTAL_TIME=0
    start = time.time()
    complete_data = []
    if list_of_stores is not None:
        excluded_stores = [x for x,check in zip(Scrapper.STORES_SCRAPPERS.keys(),list_of_stores) if not check]
    else:
        excluded_stores=[]

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
                    tmp_store_states[store_name_arg]= f"OK ({round(time.time()-start_time,2)}s)"
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


        st.session_state["complete_df"] = total_df#.astype(str)
        st.session_state["filtered_df"] = total_df#.astype(str)
        st.session_state["complete_df"].to_excel("my_silly_database.xlsx",index=False)
        st.session_state["date_of_last_pull"] = time.ctime(os.path.getmtime("my_silly_database.xlsx") + timedelta(hours=2).total_seconds())

        global COMPLETE_DATA
        #st.rerun()

    except Exception as e:
        print(f"Failed to build data: {e}")
        print(traceback.print_exc())


def try_to_retrieve_data():
    try:
        if "complete_df" not in st.session_state.keys():
            st.session_state["complete_df"] = pd.DataFrame()

        data = pd.read_excel("my_silly_database.xlsx")

        if len(st.session_state["complete_df"])!=0:
            return

        st.session_state["complete_df"] = data
        st.session_state["filtered_df"] = data
        st.session_state["date_of_last_pull"] = time.ctime(os.path.getmtime("my_silly_database.xlsx"))
        # with open("last_mod","r") as file:
        #     st.session_state['date_of_last_pull'] = file.read()

        print(st.session_state["date_of_last_pull"])

        #Refreshing statuses for stores
        if "loaded_stores" not in st.session_state.keys():
            all_pulled_stores = data["store"].to_list()
            st.session_state["loaded_stores"]={skey:"OK" if skey in all_pulled_stores else "Err" for skey in Scrapper.STORES_SCRAPPERS.keys()}


        st.rerun()
    except FileNotFoundError as e:
        #Failed to get the file
        st.warning("No data loaded :(.  Trying to do it now... (may take up to 20 seconds)")
        #Attempt to load the data
        scrap_complete_data()
        #Data is loaded


try_to_retrieve_data()


@st.dialog("Quick instruction")
def quick_instruction():


    st.markdown("\n")
    st.write(f"Once password is provided, select which stores you want to pull data from and press \\Pull data button\\")
    st.write("It'll take up to 15 secs")


# @st.dialog("Provide pass")
# def ask_for_password():
#
#     st.write(f"What's the password?")
#     reason = st.text_input("Password:")
#
#     if st.button("Submit"):
#         st.session_state["passok"] = reason=="gunlobby"
#         st.session_state["manual_read"] = False
#
#         st.rerun()

if  "manual_read" in st.session_state.keys() and st.session_state["manual_read"]:
    st.session_state["manual_read"] = True
    quick_instruction()



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

try:
    st.subheader("Stores currently available :)")
    cols = st.columns(len(st.session_state["loaded_stores"]))
    count=0
    for store,status in st.session_state["loaded_stores"].items():
        with cols[count]:
            if status=="OK":
                st.success(store)
            else:
                st.error(store)
            #st.text(f"{store} - {status}")
            count+=1
except Exception as e:
    print(e)

st.markdown("\n")


#Part responsible for selection of singular stores - disabled for now
# s_cols = st.columns(len(Scrapper.STORES_SCRAPPERS)+1)
# if "stores_checkboxes" not in st.session_state.keys():
#     st.session_state["stores_checkboxes"]={store:True for store in Scrapper.STORES_SCRAPPERS.keys()}
# choosen_stores = {}
# checkboxes = []
# for key,val in Scrapper.STORES_SCRAPPERS.items():
#     with s_cols[len(checkboxes)]:
#         x = st.checkbox(key,value=st.session_state["stores_checkboxes"][key])
#
#         checkboxes.append(x)
#
#
# def select_all():
#     if all(st.session_state["stores_checkboxes"].values()):
#         st.session_state["stores_checkboxes"] = {store:False for store in Scrapper.STORES_SCRAPPERS.keys()}
#
#     else:
#         for k,v in st.session_state["stores_checkboxes"].items():
#             st.session_state["stores_checkboxes"][k]=True
#
# with s_cols[-1]:
#     st.button("Select all",on_click=select_all)
# st.markdown("\n")
st.markdown("\n")
#if "passok" in st.session_state.keys() and st.session_state["passok"]:
st.write(f"Last data refresh: {'None' if 'date_of_last_pull' not in st.session_state.keys() else 
st.session_state['date_of_last_pull']}")
#st.button("Refresh current data", on_click=scrap_complete_data,args=[checkboxes],use_container_width=True)
st.button("Refresh current data", on_click=scrap_complete_data,args=[],use_container_width=True)
# else:
#     ask_for_password()


st.markdown("\n")
st.markdown("\n")

col1,col2 = st.columns([1,3])
with col1:

    pref_region = st.multiselect("Region",["Mazowieckie","Dolnośląskie"])

    if pref_region:

        cities = [x for xskey, xs in cities_per_region.items() for x in xs if xskey in pref_region]
        pref_city = st.multiselect("City",cities)
    else:
        pref_city = st.multiselect("City",[x for xs in  cities_per_region.values() for x in xs])




    pref_name = st.text_input("Enter complete/partial name (title)")

    # if pref_name:
    #     st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.contains(pref_name, na=False)]
    #     #print(f"New length : {len(st.session_state['filtered_df'])}")
    #
    pref_stores = st.multiselect("Preferred stores",list(Scrapper.STORES_SCRAPPERS.keys()))


    pref_size = st.multiselect("Enter preferred sizes",Scrapper.get_all_existing_sizes(st.session_state["complete_df"]))
    #
    pref_available = st.checkbox("Show only available")
    if pref_size or pref_name or pref_stores or pref_available or pref_region:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

        if pref_city:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
                st.session_state["filtered_df"]["city"].isin(pref_city)]

        if pref_name:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["title"].str.lower().str.contains(pref_name, na=False)]

        if pref_stores:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["store"].isin(pref_stores)]

        if pref_size:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["size"].isin(pref_size)]

        st.session_state["filtered_df"] = st.session_state["filtered_df"].query("available == True")


    else:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

with col2:
    #if "passok" in st.session_state.keys() and st.session_state["passok"]:
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



#if not st.session_state["complete_df"] is None:

            # def disappear(prompt):
            #     time.sleep(3)
            #     print("DISAPPEARING")
            #     prompt.empty()
            #
            # Thread(target=disappear,args=[msg]).start()

#if not st.session_state["pulled_data"]:
st.markdown("\n")



#todo pull to local df!!!