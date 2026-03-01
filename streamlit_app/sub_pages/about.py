import streamlit as st

def show_about():
    st.markdown(
        """
        <div style='text-align: center;'>
            <h1 style='color: #0073e6; font-family: monospace;'>🚀 TIKI ETL Data Pipeline</h1>
            <p style='font-size: 18px;'>End-to-End Data Engineering & Recommendation System</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.divider()

    # --- Section 1: Abstract ---
    st.header("1. Project Abstract")
    st.markdown(
        """
        This project implements a robust **ETL (Extract, Transform, Load)** pipeline that crawls product data 
        from **Tiki.vn** (Vietnam's leading e-commerce site). 
        
        The goal is to move raw, unstructured JSON data into a structured **PostgreSQL Data Warehouse** and build a **Recommendation Engine** using PySpark.
        """
    )

    # --- Section 2: Architecture ---
    st.header("2. System Architecture")
    st.markdown("Instead of static images, here is the live flow of your data:")
    
    # Using Mermaid for the Dataflow
    mermaid_code = """
    graph LR
        A[Tiki API] -->|Extract: Python| B[(MinIO Data Lake)]
        B -->|Bronze: Raw JSON| C{Apache Spark}
        C -->|Silver: Cleaned Parquet| B
        C -->|Gold: Aggregated| D[(PostgreSQL DWH)]
        D --> E[Streamlit UI]
        D --> F[Metabase BI]
        C --> G[ML Models: ALS/TF-IDF]
    """
    
    # Simple way to render mermaid in Streamlit
    st.markdown(f"```mermaid\n{mermaid_code}\n```")

    # --- Section 3: Technical Stack ---
    st.header("3. Technical Stack")
    cols = st.columns(4)
    with cols[0]:
        st.write("**Orchestration**")
        st.code("Dagster")
    with cols[1]:
        st.write("**Processing**")
        st.code("Apache Spark")
    with cols[2]:
        st.write("**Storage**")
        st.code("MinIO / Postgres")
    with cols[3]:
        st.write("**Infrastructure**")
        st.code("Docker")

    # --- Section 4: The Medallion Layers ---
    st.header("4. Data Transformation Layers")
    
    tab1, tab2, tab3 = st.tabs(["🟤 Bronze (Raw)", "⚪ Silver (Cleaned)", "🟡 Gold (Analytics)"])
    
    with tab1:
        st.subheader("Bronze Layer")
        st.write("Raw data fetched from Tiki API. Data is partitioned by category and timestamp.")
        st.code("path: warehouse/bronze/tiki/{category}/{product_id}.json")
        
    with tab2:
        st.subheader("Silver Layer")
        st.write("Data is cleaned using Spark. Schema is enforced and duplicates are removed.")
        st.markdown("- **Tables:** `products`, `sellers`, `reviews`.")
        st.markdown("- **Format:** Apache Parquet (for optimized reading).")

    with tab3:
        st.subheader("Gold Layer")
        st.write("Business-ready tables and Recommendation feature vectors.")
        st.markdown("- **DWH:** Loaded into PostgreSQL for BI tools.")
        st.markdown("- **ML:** Pre-computed similarity matrices for the Recommender.")

    # --- Section 5: Recommendation Logic ---
    st.header("5. Recommendation Engine")
    st.markdown("We employ two primary strategies:")
    
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Content-Based")
        st.write("Uses **TF-IDF** on product descriptions to find items with similar characteristics.")
        st.latex(r"score = \cos(\theta) = \frac{A \cdot B}{\|A\| \|B\|}")
    
    with col_right:
        st.subheader("Collaborative")
        st.write("Uses **ALS (Alternating Least Squares)** to predict user preferences based on past ratings.")
        st.code("model = ALS(rank=10, maxIter=10)")

    st.divider()
    st.markdown("### 🔗 Links")
    st.markdown("[![GitHub](https://img.shields.io/badge/GitHub-Repository-black?logo=github)](https://github.com/quangcaophan/tiki-elt-project)")

# Call this in your main app.py
if __name__ == "__main__":
    show_about()