import streamlit as st
from streamlit_option_menu import option_menu
from sub_pages import about, dashboard

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
    selected = option_menu('Menu', ['About', 'Data Pipeline'], 
        icons=['info-circle','graph-up','search'],menu_icon='intersect', default_index=0)
    
if selected == 'About':
    about.show_about()
elif selected == 'Data Pipeline':
    dashboard.show_dashboard()