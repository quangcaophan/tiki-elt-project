import os
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Allow running standalone or as part of the app ──────────────────────────
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.db as db

# ── Colour palette ───────────────────────────────────────────────────────────
BLUE   = "#0073e6"
TEAL   = "#00b4d8"
ORANGE = "#ff6b35"
GREEN  = "#06d6a0"
PURPLE = "#7b5ea7"
RED    = "#e63946"
GREY   = "#adb5bd"

# ── Cached queries ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_kpis():
    sql = """
        SELECT
            (SELECT COUNT(1) FROM cleaned.products)   AS total_products,
            (SELECT COUNT(1) FROM cleaned.sellers)    AS total_sellers,
            (SELECT COUNT(1) FROM cleaned.reviews)    AS total_reviews,
            (SELECT COUNT(1) FROM cleaned.categories) AS total_categories
    """
    return db.query_db(sql).iloc[0]


@st.cache_data(ttl=300)
def load_top_categories(n=15):
    sql = f"""
        SELECT
            c.name,
            COUNT(DISTINCT p.spid) AS product_count
        FROM cleaned.categories c
        JOIN cleaned.products p ON c.category_id = p.category_id
        WHERE c.is_leaf = TRUE
        GROUP BY c.name
        ORDER BY product_count DESC
        LIMIT {n}
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_price_distribution():
    sql = """
        SELECT
            CASE
                WHEN price < 50000          THEN '< 50k'
                WHEN price < 100000         THEN '50k–100k'
                WHEN price < 200000         THEN '100k–200k'
                WHEN price < 500000         THEN '200k–500k'
                WHEN price < 1000000        THEN '500k–1M'
                ELSE '1M+'
            END AS price_range,
            CASE
                WHEN price < 50000          THEN 1
                WHEN price < 100000         THEN 2
                WHEN price < 200000         THEN 3
                WHEN price < 500000         THEN 4
                WHEN price < 1000000        THEN 5
                ELSE 6
            END AS sort_order,
            COUNT(*) AS product_count
        FROM cleaned.products
        WHERE price IS NOT NULL AND price > 0
        GROUP BY price_range, sort_order
        ORDER BY sort_order
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_discount_distribution():
    sql = """
        SELECT
            CASE
                WHEN discount_rate = 0          THEN 'No discount'
                WHEN discount_rate < 10         THEN '1–9%'
                WHEN discount_rate < 20         THEN '10–19%'
                WHEN discount_rate < 30         THEN '20–29%'
                WHEN discount_rate < 50         THEN '30–49%'
                ELSE '50%+'
            END AS discount_bucket,
            CASE
                WHEN discount_rate = 0          THEN 0
                WHEN discount_rate < 10         THEN 1
                WHEN discount_rate < 20         THEN 2
                WHEN discount_rate < 30         THEN 3
                WHEN discount_rate < 50         THEN 4
                ELSE 5
            END AS sort_order,
            COUNT(*) AS product_count
        FROM cleaned.products
        WHERE discount_rate IS NOT NULL
        GROUP BY discount_bucket, sort_order
        ORDER BY sort_order
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_rating_distribution():
    sql = """
        SELECT
            rating,
            COUNT(*) AS review_count
        FROM cleaned.reviews
        WHERE rating IS NOT NULL
        GROUP BY rating
        ORDER BY rating
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_top_products(n=10):
    sql = f"""
        SELECT
            SUBSTRING(name, 1, 45) || CASE WHEN LENGTH(name) > 45 THEN '…' ELSE '' END AS name,
            all_time_quantity_sold,
            rating_average,
            price
        FROM cleaned.products
        WHERE all_time_quantity_sold IS NOT NULL
        ORDER BY all_time_quantity_sold DESC
        LIMIT {n}
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_top_sellers(n=10):
    sql = f"""
        SELECT
            name,
            total_follower,
            review_count,
            avg_rating_point,
            is_official,
            days_since_joined
        FROM cleaned.sellers
        WHERE total_follower IS NOT NULL
        ORDER BY total_follower DESC
        LIMIT {n}
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_reviews_over_time():
    sql = """
        SELECT
            DATE_TRUNC('month', purchased_at) AS month,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS avg_rating
        FROM cleaned.reviews
        WHERE purchased_at IS NOT NULL
          AND purchased_at > '2020-01-01'
          AND purchased_at < NOW()
        GROUP BY month
        ORDER BY month
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_seller_type_comparison():
    sql = """
        SELECT
            CASE WHEN is_official THEN 'Official Store' ELSE 'Third-party Seller' END AS seller_type,
            COUNT(*)                              AS seller_count,
            ROUND(AVG(avg_rating_point), 2)       AS avg_rating,
            ROUND(AVG(total_follower))             AS avg_followers,
            ROUND(AVG(days_since_joined))          AS avg_days_joined
        FROM cleaned.sellers
        GROUP BY is_official
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_inventory_status():
    sql = """
        SELECT
            COALESCE(NULLIF(inventory_status, ''), 'unknown') AS status,
            COUNT(*) AS product_count
        FROM cleaned.products
        GROUP BY status
        ORDER BY product_count DESC
    """
    return db.query_db(sql)


@st.cache_data(ttl=300)
def load_rating_vs_price():
    sql = """
        SELECT
            ROUND(price / 10000) * 10000    AS price_bucket,
            ROUND(AVG(rating_average), 2)   AS avg_rating,
            COUNT(*)                        AS product_count
        FROM cleaned.products
        WHERE price > 0 AND price < 1000000
          AND rating_average > 0
        GROUP BY price_bucket
        HAVING COUNT(*) >= 5
        ORDER BY price_bucket
    """
    return db.query_db(sql)


# ── Helpers ──────────────────────────────────────────────────────────────────

def kpi_card(label, value, delta=None):
    """Render a single KPI card using st.metric."""
    st.metric(label=label, value=value, delta=delta)


# ── Main page ─────────────────────────────────────────────────────────────────

def show_dashboard():
    st.markdown(
        """
        <h2 style='color:#0073e6; font-family:monospace;'>📊 Tiki Analytics Dashboard</h2>
        """,
        unsafe_allow_html=True,
    )
    st.caption("All data sourced from the `cleaned` schema in the PostgreSQL data warehouse.")
    st.divider()

    # ── KPIs ─────────────────────────────────────────────────────────────────
    try:
        kpis = load_kpis()
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            kpi_card("📦 Total Products",   f"{int(kpis.total_products):,}")
        with c2:
            kpi_card("🏪 Total Sellers",    f"{int(kpis.total_sellers):,}")
        with c3:
            kpi_card("💬 Total Reviews",    f"{int(kpis.total_reviews):,}")
        with c4:
            kpi_card("🗂️ Total Categories", f"{int(kpis.total_categories):,}")
    except Exception as e:
        st.error(f"Could not load KPIs: {e}")

    st.divider()

    # ── Row 1: Top categories  +  Inventory status ───────────────────────────
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("🏷️ Top 15 Leaf Categories by Product Count")
        try:
            df_cat = load_top_categories()
            fig = px.bar(
                df_cat.sort_values("product_count"),
                x="product_count", y="name",
                orientation="h",
                color="product_count",
                color_continuous_scale=[[0, TEAL], [1, BLUE]],
                labels={"product_count": "Products", "name": ""},
            )
            fig.update_layout(
                coloraxis_showscale=False,
                margin=dict(l=0, r=10, t=10, b=10),
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    with col_right:
        st.subheader("📦 Inventory Status")
        try:
            df_inv = load_inventory_status()
            fig = px.pie(
                df_inv, values="product_count", names="status",
                color_discrete_sequence=[GREEN, BLUE, ORANGE, GREY, RED],
                hole=0.45,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(
                showlegend=False,
                margin=dict(l=10, r=10, t=10, b=10),
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    # ── Row 2: Price distribution  +  Discount distribution ──────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("💰 Price Distribution")
        try:
            df_price = load_price_distribution()
            fig = px.bar(
                df_price, x="price_range", y="product_count",
                color="product_count",
                color_continuous_scale=[[0, TEAL], [1, BLUE]],
                labels={"price_range": "Price (VND)", "product_count": "Products"},
            )
            fig.update_layout(
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=10),
                height=320,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    with col_right:
        st.subheader("🏷️ Discount Rate Distribution")
        try:
            df_disc = load_discount_distribution()
            colors = [GREY, TEAL, GREEN, ORANGE, RED, PURPLE]
            fig = px.bar(
                df_disc, x="discount_bucket", y="product_count",
                color="discount_bucket",
                color_discrete_sequence=colors,
                labels={"discount_bucket": "Discount", "product_count": "Products"},
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=0, r=0, t=10, b=10),
                height=320,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    # ── Row 3: Rating distribution ────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("⭐ Review Rating Distribution")
        try:
            df_rat = load_rating_distribution()
            star_colors = {1: RED, 2: ORANGE, 3: "#f4d03f", 4: TEAL, 5: GREEN}
            df_rat["color"] = df_rat["rating"].map(star_colors)
            fig = px.bar(
                df_rat, x="rating", y="review_count",
                color="rating",
                color_discrete_map=star_colors,
                labels={"rating": "Stars", "review_count": "Reviews"},
                text="review_count",
            )
            fig.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig.update_layout(
                showlegend=False,
                xaxis=dict(tickmode="linear", tick0=1, dtick=1),
                margin=dict(l=0, r=0, t=10, b=10),
                height=320,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    with col_right:
        st.subheader("📈 Avg Rating vs. Price")
        try:
            df_rvp = load_rating_vs_price()
            df_rvp["price_k"] = (df_rvp["price_bucket"] / 1000).astype(int).astype(str) + "k"
            fig = px.scatter(
                df_rvp, x="price_bucket", y="avg_rating",
                size="product_count",
                color="avg_rating",
                color_continuous_scale=[[0, ORANGE], [0.5, TEAL], [1, GREEN]],
                labels={
                    "price_bucket": "Price (VND)",
                    "avg_rating": "Avg Rating",
                    "product_count": "# Products",
                },
                hover_data={"product_count": True},
            )
            fig.update_layout(
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=10),
                height=320,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    # ── Row 4: Reviews over time ──────────────────────────────────────────────
    st.subheader("📅 Review Volume & Average Rating Over Time")
    try:
        df_time = load_reviews_over_time()
        if not df_time.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(
                    x=df_time["month"], y=df_time["review_count"],
                    name="Review count", marker_color=BLUE, opacity=0.7,
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=df_time["month"], y=df_time["avg_rating"],
                    name="Avg rating", line=dict(color=ORANGE, width=2.5),
                    mode="lines+markers",
                ),
                secondary_y=True,
            )
            fig.update_yaxes(title_text="Review Count", secondary_y=False)
            fig.update_yaxes(title_text="Avg Rating", range=[1, 5], secondary_y=True)
            fig.update_layout(
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=0, r=0, t=30, b=10),
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No time-series data available.")
    except Exception as e:
        st.error(f"Error: {e}")

    # ── Row 5: Top products  +  Top sellers ───────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🏆 Top 10 Products by All-Time Sales")
        try:
            df_top = load_top_products()
            fig = px.bar(
                df_top.sort_values("all_time_quantity_sold"),
                x="all_time_quantity_sold", y="name",
                orientation="h",
                color="rating_average",
                color_continuous_scale=[[0, ORANGE], [0.5, TEAL], [1, GREEN]],
                labels={
                    "all_time_quantity_sold": "Units Sold",
                    "name": "",
                    "rating_average": "Rating",
                },
                hover_data={"price": ":,.0f"},
            )
            fig.update_layout(
                margin=dict(l=0, r=10, t=10, b=10),
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    with col_right:
        st.subheader("🏪 Top 10 Sellers by Followers")
        try:
            df_sellers = load_top_sellers()
            df_sellers["type_color"] = df_sellers["is_official"].map(
                {True: GREEN, False: ORANGE}
            )
            df_sellers["type_label"] = df_sellers["is_official"].map(
                {True: "Official", False: "3rd-party"}
            )
            fig = px.bar(
                df_sellers.sort_values("total_follower"),
                x="total_follower", y="name",
                orientation="h",
                color="type_label",
                color_discrete_map={"Official": GREEN, "3rd-party": ORANGE},
                labels={
                    "total_follower": "Followers",
                    "name": "",
                    "type_label": "Type",
                },
                hover_data={"avg_rating_point": True, "review_count": ":,"},
            )
            fig.update_layout(
                margin=dict(l=0, r=10, t=10, b=10),
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

    # ── Row 6: Official vs third-party seller comparison ─────────────────────
    st.subheader("⚖️ Official Stores vs. Third-Party Sellers")
    try:
        df_cmp = load_seller_type_comparison()
        if not df_cmp.empty:
            metrics = ["avg_rating", "avg_followers", "avg_days_joined"]
            labels  = ["Avg Rating (×1000)", "Avg Followers", "Avg Days on Platform"]

            # normalise for radar — scale avg_rating by 1000 so it's visible alongside others
            df_radar = df_cmp.copy()
            df_radar["avg_rating"] = df_radar["avg_rating"] * 1000

            fig = go.Figure()
            colors_radar = [GREEN, ORANGE]
            for i, row in df_radar.iterrows():
                fig.add_trace(go.Scatterpolar(
                    r=[row[m] for m in metrics] + [row[metrics[0]]],
                    theta=labels + [labels[0]],
                    fill="toself",
                    name=row["seller_type"],
                    line_color=colors_radar[i % 2],
                    opacity=0.6,
                ))
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True)),
                showlegend=True,
                height=350,
                margin=dict(l=40, r=40, t=20, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)

            # summary table
            st.dataframe(
                df_cmp.rename(columns={
                    "seller_type":      "Type",
                    "seller_count":     "# Sellers",
                    "avg_rating":       "Avg Rating",
                    "avg_followers":    "Avg Followers",
                    "avg_days_joined":  "Avg Days Active",
                }),
                use_container_width=True,
                hide_index=True,
            )
    except Exception as e:
        st.error(f"Error: {e}")