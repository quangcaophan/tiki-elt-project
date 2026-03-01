import streamlit as st
from streamlit_option_menu import option_menu
from sub_pages.about import show_about

st.set_page_config(
    page_title="Tiki Recommender ETL Pipeline System",
    layout="wide",
    page_icon=":mag_right:",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.big-font {
    font-size:80px !important;s
}
</style>
""", unsafe_allow_html=True)



with st.sidebar:
    selected = option_menu('Menu', ['About'], 
        icons=['info-circle','graph-up','search'],menu_icon='intersect', default_index=0)
    
if True:
    show_about()