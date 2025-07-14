#Main class to manage rest of the code
import concurrent
import locale
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
import Scrapper

st.set_page_config(layout="wide")

#locale.setlocale(locale.LC_TIME, 'pl_PL.UTF-8')
logging.getLogger('streamlit.runtime.scriptrunner').setLevel(logging.ERROR)

cities_per_region = {"Mazowieckie":["Warszawa","Płock","Pruszków","Siedlce","Ostrołęka","Ciechanów","Siedlce"],
                     "Łódzkie":["Łódź","Piotrków Trybunalski","Pabianice","Aleksandrów Łódzki","Bełchatów"],
                     "Wielkopolskie": ["Poznań","Śrem"],
                     "Dolnośląskie":["Wrocław","Mirków"],
                     "Małopolskie":["Kraków"],
                     "Lubelskie":["Lublin"],
                     "Podkarpackie":["Jasło"],
                     "Śląskie":["Jaworzno","Częstochowa"],
                     "Zachodniopomorskie":["Kołobrzeg"]}

# if "loaded_stores" not in st.session_state.keys():
#     all_pulled_stores = list(Scrapper.STORES_SCRAPPERS.keys())
#     stores_in_datast = list(st.session_state.keys())
#     print("Loaded stores not in session state")
#     st.session_state["loaded_stores"] = {skey:  "Err"
#                                          for skey in
#                                          Scrapper.STORES_SCRAPPERS.keys()}

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

def normalize_data(df:list):
    total_df = pd.DataFrame(df)
    total_df = total_df[["Miasto", "Tytuł", "Cena", "Link", "Kaliber", "Dostępny", "Sklep"]]
    total_df = Scrapper.map_sizes(total_df)

    total_df = Scrapper.map_prices(total_df)

    exclude_regex = r"([Pp]ude[lł]ko)|(SZKOLENIE)"

    total_df = total_df[~total_df['Tytuł'].str.contains(exclude_regex,regex=True)]


    total_df.columns = ["Miasto","Tytuł","Cena","Link","Kaliber","Dostępny","Sklep"]

    def drop_all_odd(value):
        subval = re.subn(r"[^0-9,\\.]", "", value, count=1)[0].replace(".00", "")
        subval = re.sub("\\.{2}[0-9]+$", "", subval)
        if subval.count(".") == 2:
            subval = re.sub(r"\.[0-9]{2}\.?$", "", subval)

        return subval

    # total_df.to_excel("tmp.xlsx", index=False)

    total_df["Cena"] = pd.to_numeric(total_df["Cena"].fillna('').apply(lambda x: drop_all_odd(x)),
                                      errors='coerce')  # .fillna('-1')

    total_df['Dostępny'] = total_df['Dostępny'].apply(lambda x: "T" if x  else ("N" if not x else "?"))

    #adjusting prices for box size (if available)
    total_df = Scrapper.map_prices_by_box_size(total_df)


    #re.sub(r"\.", "", "aa.bb.c")
    return total_df


def scrap_complete_data(list_of_stores:list=None):

    # if "adminpass" not in st.session_state.keys():
    #     ask_for_password()
    #     return

    if "loaded_stores" not in st.session_state.keys():
        st.session_state["loaded_stores"] = {}
    st.toast("Odświeżanie danych rozpoczęte. Może zająć do kilku minut. Cierpliwości :)")
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

    #for store_name, store_scrap in Scrapper.STORES_SCRAPPERS.items():

    def pull_single_store(store_name_arg):
        start_time = time.time()
        try:

            res = Scrapper.STORES_SCRAPPERS[store_name_arg]()
            try:
                complete_data.extend(res)
            except Exception as e:
                print(e)
                print(f"Failure for: {store_name_arg}")
            #st.session_state["pulled_data"][store_name_arg] = res
            if not res:
                st.toast(f"ERROR - Failed to scrap {store_name_arg}")

                tmp_store_states[store_name_arg]= "ERROR"
            else:
                st.toast(f"OK - Successfully scrapped {store_name_arg}")

                tmp_store_states[store_name_arg]= f"OK ({round(time.time()-start_time,2)}s)"

        except Exception as e:

            print(traceback.print_exc())
            if "loaded_stores" not in st.session_state.keys():
                st.session_state["loaded_stores"] = {}
            msg = st.toast(f"ERROR - Failed to scrap {store_name_arg} data({round(time_format(start_time))}s)")
            st.session_state["loaded_stores"][store_name_arg] = "ERROR"

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(pull_single_store, list(Scrapper.STORES_SCRAPPERS.keys()))


    #pull_single_store(store_name)

    # while any([t.is_alive() for t in thread_list]):
    #     time.sleep(1)

    #DATA_PULL_TOTAL_TIME = Scrapper.
    #st.session_state["complete_data"]
    st.session_state["loaded_stores"] = tmp_store_states
    st.success(f"Aktualizacja danych zajeła: {round(time.time()-start,2)}s")
    try:


        total_df = normalize_data(complete_data)



        st.session_state["complete_df"] = total_df#.astype(str)
        st.session_state["filtered_df"] = total_df#.astype(str)

        st.session_state["complete_df"].to_excel("my_silly_database.xlsx",index=False)
        st.session_state["date_of_last_pull"] = time.ctime(os.path.getmtime("my_silly_database.xlsx") + timedelta(hours=2).total_seconds())
        st.balloons()
        st.session_state["loaded_stores"] = {skey: "OK" if skey in st.session_state["complete_df"]["Sklep"].to_list() else "Err" for skey in
                                             Scrapper.STORES_SCRAPPERS.keys()}
        global COMPLETE_DATA

        #st.rerun()

    except Exception as e:
        print(f"Failed to build data: {e}")
        print(traceback.print_exc())


def try_to_retrieve_data():
    try:
        if "complete_df" not in st.session_state.keys():
            st.session_state["complete_df"] = pd.DataFrame(columns=["Miasto","Tytuł","Cena","Link","Kaliber","Sklep","Dostępny"])



        data = pd.read_excel("my_silly_database.xlsx")




        # if len(st.session_state["complete_df"])!=0:
        #     return

        st.session_state["complete_df"] = data
        st.session_state["filtered_df"] = data
        st.session_state["date_of_last_pull"] = time.ctime(os.path.getmtime("my_silly_database.xlsx"))
        st.session_state["date_of_last_codechange"] = max(
            [time.ctime(os.path.getmtime("StreamlitLaunchpad.py")),
             time.ctime(os.path.getmtime("Scrapper.py"))])

        # with open("last_mod","r") as file:
        #     st.session_state['date_of_last_pull'] = file.read()


        st.session_state["loaded_stores"] = {skey: "OK" if skey in list(set(st.session_state["complete_df"]["Sklep"].to_list())) else "Err" for skey in
                                             Scrapper.STORES_SCRAPPERS.keys()}

        #Refreshing statuses for stores


        #st.rerun()
    except FileNotFoundError as e:
        #Failed to get the file
        st.warning("No data loaded :(.  Trying to do it now... (may take up to couple of minutes)")
        #Attempt to load the data
        scrap_complete_data()
        #Data is loaded


try_to_retrieve_data()



@st.dialog("Polskie pestki")
def basic_info_prompt(info):
    st.write(info)

@st.dialog("Polskie pestki")
def ask_for_password():
    if "admin" not in st.session_state.keys():
        title = st.write(f"Podaj hasło admina")
        reason = st.text_input("")
        submit = st.button("Submit")

        if submit:
            if not reason=="gunlobby":
                st.toast("Tylko admin może odświeżać dane")
            else:
                st.toast("Hasło zaakceptowane!")
                st.session_state["adminpass"]=True
                st.session_state["download_order"]=True

                st.rerun()
    else:
        st.session_state["download_order"] = True

        st.rerun()


# if  "manual_read" in st.session_state.keys() and st.session_state["manual_read"]:
#     st.session_state["manual_read"] = True
#     quick_instruction()



# Title
st.title("🌱 Polskie pestki 🌱")

st.subheader(f"Zebrane ceny amunicji z {len(Scrapper.STORES_SCRAPPERS)} sklepów w Polsce!")

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

st.markdown("\n")

st.markdown("\n")

st.write(f"Ostatnie odświeżenie danych: {'Brak' if 'date_of_last_pull' not in st.session_state.keys() else 
st.session_state['date_of_last_pull']}")

st.markdown("\n")
st.markdown("\n")

col1,col2 = st.columns([1,3])
with col1:

    pref_region = st.multiselect("Województwo",cities_per_region.keys(),placeholder="Województwo")

    if pref_region:

        cities = [x for xskey, xs in cities_per_region.items() for x in xs if xskey in pref_region]
        pref_city = st.multiselect("Miasto",cities,placeholder="Miasto")
    else:
        pref_city = st.multiselect("Miasto",[x for xs in  cities_per_region.values() for x in xs],placeholder="Miasto")




    pref_name = st.text_input("Podaj pełną/częściową nazwę",placeholder="")

    pref_stores = st.multiselect("Wybrane sklepy",sorted(list(Scrapper.STORES_SCRAPPERS.keys())),placeholder="")


    pref_size = st.multiselect("Wybierz kaliber/rozmiar",Scrapper.get_all_existing_sizes(st.session_state["complete_df"]),placeholder="")
    #
    pref_available = st.checkbox("Pokaż tylko dostępne",help="Zaznacz aby wyświetlić tylko dostępne produkty. Produkty niedostępne oraz bez znanego statusu (sklep nie udostępnia informacji) będą ukryte)")

    if pref_size or pref_name or pref_stores or pref_available or pref_region or pref_city:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

        if pref_city:

            st.session_state["filtered_df"] = st.session_state["filtered_df"][
                st.session_state["filtered_df"]["Miasto"].isin(pref_city)]

        if pref_name:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["Tytuł"].str.lower().str.contains(pref_name, na=False)]

        if pref_stores:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["Sklep"].isin(pref_stores)]

        if pref_size:
            st.session_state["filtered_df"] = st.session_state["filtered_df"][
            st.session_state["filtered_df"]["Kaliber"].isin(pref_size)]

        if pref_available:
            st.session_state["filtered_df"] = st.session_state["filtered_df"].query("Dostępny == 'T'")

        if pref_region:
            #proper_cities

            st.session_state["filtered_df"] = st.session_state["filtered_df"][st.session_state["filtered_df"]["Miasto"].isin(cities)]


    else:
        st.session_state["filtered_df"] = st.session_state["complete_df"]

with col2:
    st.dataframe(st.session_state["filtered_df"],
                 column_config={"Link": st.column_config.LinkColumn(
            "Link", display_text="Oferta"
        ),

    })
    st.text(f"Ilość przefiltrowanych rekordów: {len(st.session_state["filtered_df"])}")
    st.text(f"Całkowita ilość rekordów: {len(st.session_state["complete_df"])}")


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

st.markdown("\n")




try:
    st.subheader("Obecnie dostępne sklepy :)")
    total_amount_of_stores = len(st.session_state["loaded_stores"])
    cols = st.columns(8)
    count=0

    stores = list(st.session_state["loaded_stores"].items())
    stores.sort(key=lambda x:x[0].lower())
    for store,status in stores:
        if count==8:
            count=0
            cols=st.columns(8)
        with cols[count]:
            if status=="OK":
                st.success(store)
            else:
                st.error(store)
            #st.text(f"{store} - {status}")
            count+=1

except Exception as e:
    print(e)

#For admin access (refresh prompt)
if "admin" in st._get_query_params().keys():
    st.button("Odśwież dane", on_click=ask_for_password,args=[],use_container_width=True,help= "Naciśnij aby przeładować dane (powinno zająć do czterech minut)" )

#If admin is verified and password confirmed - refresh data
if "download_order" in st.session_state.keys() and st.session_state["download_order"]:
    scrap_complete_data()
    st.session_state["download_order"]= False
    basic_info_prompt(f"Dane odświeżone! Ilość pobranych ofert: {len(st.session_state['complete_df'])}" )
    st.rerun()


st.text("Masz uwagi? Brakuje sklepu? Coś może działać lepiej? Daj cynk na astorbeon@protonmail.com!")

if "was_informed_about_wip" not in st.session_state:
    basic_info_prompt(f'Praca wre! Strona wciąż jest w budowie, mogą zdarzać się błędy. Ostatnia aktualizacja kodu: {st.session_state["date_of_last_codechange"]}')
    st.session_state["was_informed_about_wip"]=True