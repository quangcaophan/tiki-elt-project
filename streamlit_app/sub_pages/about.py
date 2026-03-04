import streamlit as st


def show_about():
    # ── Hero ──────────────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style='text-align: center; padding: 1rem 0 0.5rem 0;'>
            <h1 style='color: #0073e6; font-family: monospace; font-size: 2.6rem;'>
                🛒 TIKI Recommender ETL Pipeline
            </h1>
            <p style='font-size: 1.1rem; color: #555;'>
                End-to-end data engineering on Vietnam's largest e-commerce bookstore
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    # ── 1. Abstract ───────────────────────────────────────────────────────────
    st.header("1. Abstract")
    st.markdown(
        """
        This project builds a production-grade **ETL pipeline** that continuously harvests
        product, seller, and review data from [Tiki.vn](https://tiki.vn/) — Vietnam's
        leading e-commerce platform — and turns it into a structured **PostgreSQL Data
        Warehouse** ready for analytics and a **book recommendation engine**.

        The pipeline covers the full data engineering lifecycle:

        - **Extract** — concurrent HTTP crawlers hit Tiki's internal APIs and persist raw
          JSON responses directly into a PostgreSQL `raw` schema (acts as the landing zone).
        - **Transform** — [dbt](https://www.getdbt.com/) models clean, deduplicate and
          enforce schema, promoting data to a `cleaned` schema with proper primary/foreign
          keys and incremental loading.
        - **Load / Serve** — cleaned tables are exposed to a Streamlit dashboard for
          interactive recommendations and to Metabase for BI dashboards.
        - **Orchestrate** — Apache Airflow schedules four independent DAGs (categories,
          products, sellers, reviews) and wires each crawl directly to its dbt model.
        """
    )

    # ── 2. Architecture ───────────────────────────────────────────────────────
    st.header("2. System Architecture")

    st.markdown(
        """
        ```mermaid
        flowchart TD
            subgraph Extract["🔍 Extract  (Python crawlers)"]
                A1[Tiki Categories API]
                A2[Tiki Product Listings API]
                A3[Tiki Seller API]
                A4[Tiki Reviews API]
            end

            subgraph Raw["🗄️ raw schema  (PostgreSQL — landing zone)"]
                B1[(raw_categories)]
                B2[(raw_product_listings)]
                B3[(raw_sellers)]
                B4[(raw_reviews)]
            end

            subgraph Transform["⚙️ Transform  (dbt incremental models)"]
                C1[categories.sql]
                C2[products.sql]
                C3[sellers.sql]
                C4[reviews.sql]
                C5[product_categories.sql]
            end

            subgraph Cleaned["🏛️ cleaned schema  (PostgreSQL — DWH)"]
                D1[(categories)]
                D2[(products)]
                D3[(sellers)]
                D4[(reviews)]
                D5[(product_categories)]
            end

            subgraph Serve["📊 Serve"]
                E1[Streamlit App]
                E2[Metabase Dashboard]
            end

            subgraph Orchestrate["🎼 Apache Airflow"]
                F1[tiki_categories_etl  — weekly]
                F2[tiki_products_etl  — daily 00:30]
                F3[tiki_seller_etl  — weekly]
                F4[tiki_review_etl  — daily 01:30]
            end

            A1 --> B1 --> C1 --> D1
            A2 --> B2 --> C2 --> D2
            A2 --> B2 --> C5 --> D5
            A3 --> B3 --> C3 --> D3
            A4 --> B4 --> C4 --> D4

            D2 & D4 --> E1
            D1 & D2 & D3 & D4 --> E2

            F1 -.-> A1
            F2 -.-> A2
            F3 -.-> A3
            F4 -.-> A4
        ```
        """
    )

    # ── 3. Tech Stack ─────────────────────────────────────────────────────────
    st.header("3. Technical Stack")

    cols = st.columns(4)
    stack = [
        ("🎼 Orchestration", "Apache Airflow 2.7"),
        ("🔄 Transform",     "dbt-postgres"),
        ("🗄️ Storage",       "PostgreSQL 13"),
        ("🐳 Infrastructure","Docker Compose"),
    ]
    for col, (label, tool) in zip(cols, stack):
        with col:
            st.markdown(f"**{label}**")
            st.code(tool, language=None)

    cols2 = st.columns(4)
    stack2 = [
        ("🕷️ Crawling",      "requests + ThreadPoolExecutor"),
        ("📈 ML / Reco",     "PySpark ALS + TF-IDF"),
        ("🖥️ Dashboard",     "Streamlit + Metabase"),
        ("🔗 ORM / DB",      "SQLAlchemy + psycopg2"),
    ]
    for col, (label, tool) in zip(cols2, stack2):
        with col:
            st.markdown(f"**{label}**")
            st.code(tool, language=None)

    # ── 4. Data Pipeline Detail ───────────────────────────────────────────────
    st.header("4. Pipeline Detail")

    # 4.1 Extract
    with st.expander("4.1  Extract — Concurrent Crawlers", expanded=True):
        st.markdown(
            """
            Four independent Python modules (`raw_categories`, `raw_products`,
            `raw_sellers`, `raw_reviews`) share the same design pattern:

            1. **Probe page 1** of every category/product concurrently to discover
               `last_page` from the `paging` object in the API response.
            2. **Fan out** remaining pages and fetch them in parallel using
               `ThreadPoolExecutor`.
            3. **Stream results** through a `Queue` into a dedicated **DB writer thread**
               that upserts in configurable batches — keeping RAM usage flat even for
               large crawls.

            A shared `RateLimiter` (semaphore + min-delay) prevents the crawler from
            hammering Tiki and triggers exponential back-off on `429 / 403 / 503`
            responses.
            """
        )
        st.markdown(
            """
            ```mermaid
            sequenceDiagram
                participant Main
                participant ThreadPool
                participant Queue
                participant DBWriter

                Main->>ThreadPool: submit probe tasks (page 1)
                ThreadPool-->>Queue: enqueue results
                Queue-->>DBWriter: upsert batch → raw schema
                Main->>Main: build remaining page tasks from last_page
                Main->>ThreadPool: submit remaining tasks
                ThreadPool-->>Queue: enqueue results
                Queue-->>DBWriter: upsert final batches
                Main->>Queue: send poison pill (None)
                DBWriter-->>Main: done
            ```
            """
        )

        st.markdown("**Raw schema layout (PostgreSQL landing zone):**")
        st.code(
            """
raw.raw_categories       PRIMARY KEY (categories_id)
raw.raw_product_listings PRIMARY KEY (category_id, page)
raw.raw_sellers          PRIMARY KEY (seller_id)
raw.raw_reviews          PRIMARY KEY (spid, page)

Each row stores: extract_time  +  raw_response JSONB
            """,
            language="sql",
        )

    # 4.2 Transform
    with st.expander("4.2  Transform — dbt Incremental Models"):
        st.markdown(
            """
            dbt unpacks the `JSONB` columns into typed, relational tables inside the
            `cleaned` schema.  All models are **incremental** — they only process rows
            not already present in the target table — making daily runs fast even as
            the raw layer grows.

            | dbt model | Source table | Key |
            |---|---|---|
            | `categories` | `raw_categories` | `category_id` |
            | `sellers` | `raw_sellers` | `seller_id` |
            | `products` | `raw_product_listings` | `spid` (seller-product ID) |
            | `product_categories` | `raw_product_listings` | `(spid, category_id)` |
            | `reviews` | `raw_reviews` | `review_id` |

            dbt also manages **foreign-key constraints** via `pre_hook` / `post_hook`
            so the DWH stays referentially consistent across incremental loads.
            """
        )
        st.markdown(
            """
            ```mermaid
            erDiagram
                categories {
                    int category_id PK
                    int parent_id FK
                    varchar name
                    int level
                    bool is_leaf
                }
                sellers {
                    int seller_id PK
                    varchar name
                    numeric avg_rating_point
                    bool is_official
                }
                products {
                    bigint spid PK
                    bigint product_id
                    int seller_id FK
                    varchar name
                    numeric price
                    numeric rating_average
                    int review_count
                }
                reviews {
                    bigint review_id PK
                    bigint spid FK
                    int seller_id FK
                    bigint user_id
                    int rating
                    text content
                }
                product_categories {
                    bigint spid FK
                    int category_id FK
                }

                sellers ||--o{ products : "sells"
                products ||--o{ reviews : "has"
                products ||--o{ product_categories : "belongs to"
                categories ||--o{ product_categories : "contains"
                categories ||--o{ categories : "parent"
            ```
            """
        )

    # 4.3 Orchestration
    with st.expander("4.3  Orchestration — Apache Airflow DAGs"):
        st.markdown(
            """
            | DAG | Schedule | Tasks |
            |---|---|---|
            | `tiki_categories_etl` | Every Sunday midnight | crawl → dbt `categories` |
            | `tiki_products_etl` | Daily 00:30 | crawl → dbt `products` → dbt `product_categories` |
            | `tiki_seller_etl` | Every Sunday 01:00 | crawl → dbt `sellers` |
            | `tiki_review_etl` | Daily 01:30 | crawl → dbt `reviews` |

            Each DAG follows the same two-step pattern:

            ```
            PythonOperator (crawl + upsert raw)  ──►  BashOperator (dbt run --select <model>)
            ```

            Products run before sellers and reviews so foreign-key dependencies are
            always satisfied.
            """
        )

    # ── 5. Recommendation Engine ──────────────────────────────────────────────
    st.header("5. Recommendation Engine")

    tab_cbf, tab_cf = st.tabs(["📄 Content-Based Filtering", "👥 Collaborative Filtering"])

    with tab_cbf:
        st.markdown(
            """
            Product titles and descriptions are tokenised after stopword removal.
            **TF-IDF** vectors are built per product and pairwise **Gensim cosine
            similarity** is used to rank candidates.

            A search query goes through the same vectorisation step so it can be
            compared directly against the product index.
            """
        )
        st.markdown("**TF-IDF formulas:**")
        st.latex(r"TF(t,d) = \frac{f_{t,d}}{\sum_{t' \in d} f_{t',d}}")
        st.latex(r"IDF(t)   = \log\!\left(\frac{N}{1 + DF(t)}\right)")
        st.latex(r"TF\text{-}IDF(t,d) = TF(t,d) \times IDF(t)")

    with tab_cf:
        st.markdown(
            """
            **ALS (Alternating Least Squares)** in PySpark is trained on the
            `(customer_id, product_id, rating)` triples from the `reviews` table.
            ALS handles sparse implicit feedback well and scales to the full dataset
            with Spark's distributed execution.
            """
        )
        st.code(
            """
from pyspark.ml.recommendation import ALS

als = ALS(
    userCol          = "customer_id",
    itemCol          = "item_index",
    ratingCol        = "rating",
    coldStartStrategy= "drop",
    rank             = 10,
    maxIter          = 10,
)
model = als.fit(gold_reviews)
            """,
            language="python",
        )

    # ── 6. Source Code ────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        """
        ### 🔗 Source Code
        Full source on GitHub →
        [**nchn471/tiki-recommender-etl-pipeline**](https://github.com/nchn471/tiki-recommender-etl-pipeline)
        """,
        unsafe_allow_html=False,
    )