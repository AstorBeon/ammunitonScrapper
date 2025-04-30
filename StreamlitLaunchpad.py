#Main class to manage rest of the code
import time
from threading import Thread

import streamlit as st
import pandas as pd
import Scrapper
from Scrapper import STORES_SCRAPPERS

# Title
st.title("Find ammo in Warsaw!")
st.subheader("Nifty scrapper collecting data reg ammo prices in Warsaw")
col1,col2 = st.columns([1,3])
st.session_state["pulled_data"]={}
st.session_state["complete_df"]=None


def scrap_complete_data():
    complete_data = []

    for store_name, store_scrap in Scrapper.STORES_SCRAPPERS.items():
        try:
            res = store_scrap()
            complete_data.extend(res)
            st.session_state["pulled_data"][store_name] = res
            if not res:
                msg = st.error(f"Failed to scrap {store_name} data")
            else:
                msg = st.success(f"Successfully scrapped {store_name} data");
        except Exception as e:
            msg = st.error(f"Failed to scrap {store_name} data")
            print(e)
    print("Data loaded")
    print(complete_data)
    try:
        print(f"Precast")
        total_df = pd.DataFrame(complete_data)
        print(total_df)
        st.session_state["complete_df"] = total_df
        total_df.to_excel(r"C:\Users\macie\Downloads\complete_data.xlsx")
        with col2:
            #pass
            st.dataframe(total_df, use_container_width=True)
    except Exception as e:
        print(e)

with col1:
    try:
        st.subheader("Available pages for scrapping")
        st.text(f"Strefa celu - {'OK' if 'Strefa celu' in st.session_state['pulled_data'].keys() else '?'}")
        st.text(f"Garand - {'OK' if 'Garad' in st.session_state['pulled_data'].keys() else '?'}")
        st.text(f"Top Gun - {'OK' if 'Top Gun' in st.session_state['pulled_data'].keys() else '?'}")



                # def disappear(prompt):
                #     time.sleep(3)
                #     print("DISAPPEARING")
                #     prompt.empty()
                #
                # Thread(target=disappear,args=[msg]).start()

        st.button("Pull current data", on_click=scrap_complete_data)
    except Exception as e:
        print(e)








