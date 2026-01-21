from flask import Flask, request
import folium
import pandas as pd
import mysql.connector
import re

app = Flask(__name__)

def normalize(text):
    if isinstance(text, str):
        return re.sub(r'[^a-z0-9]+', '', text.lower())
    return ""

# connect to MySQL
def load_data():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        database="yt169"
    )

    engagement_df = pd.read_sql(
        """
        SELECT country, title, engagement_rate
        FROM engagement_metrics
        ORDER BY engagement_rate DESC
        """,
        conn
    )

    categories_df = pd.read_sql(
        "SELECT country, category, num_categories FROM category_count",
        conn
    )

    tags_df = pd.read_sql(
        "SELECT country, tag, num_tags FROM tag_count",
        conn
    )

    conn.close()
    return engagement_df, categories_df, tags_df

@app.route("/")
def map_page():
    # selects top N rows (5, 10, 15)
    top_n = request.args.get("top", default=5, type=int)

    # SEARCH PARAMETERS
    search_query = request.args.get("search", default="", type=str).strip().lower()
    search_table = request.args.get("table", default="", type=str)
    search_country = request.args.get("country", default="", type=str)

    # read data from MySQL
    eng_df, cat_df, tags_df = load_data()

    top_engagement = {}
    for country in eng_df["country"].unique():
        subset = eng_df[eng_df["country"] == country].sort_values("engagement_rate", ascending=False)
        top_rows = subset.head(top_n)

        lines = []
        for _, row in top_rows.iterrows():
            lines.append(f"{row['title']} — {round(row['engagement_rate'], 4)}")

        top_engagement[country] = "<br><br>".join(lines)

    top_categories = {}
    for country in cat_df["country"].unique():
        subset = cat_df[cat_df["country"] == country].sort_values("num_categories", ascending=False)
        top_rows = subset.head(top_n)

        lines = [f"{row['category']} — {row['num_categories']}" for _, row in top_rows.iterrows()]
        top_categories[country] = "<br>".join(lines)

    top_tags = {}
    for country in tags_df["country"].unique():
        subset = tags_df[tags_df["country"] == country].sort_values("num_tags", ascending=False)
        top_rows = subset.head(top_n)

        lines = [f"{row['tag']} — {row['num_tags']}" for _, row in top_rows.iterrows()]
        top_tags[country] = "<br>".join(lines)

    # coordinates for countries
    coords = {
        "CA": [56.1304, -106.3468],
        "DE": [51.1657, 10.4515],
        "FR": [46.2276, 2.2137],
        "GB": [55.3781, -3.4360],
        "IN": [20.5937, 78.9629],
        "JP": [36.2048, 138.2529],
        "KR": [35.9078, 127.7669],
        "MX": [23.6345, -102.5528],
        "RU": [61.5240, 105.3188],
        "US": [37.0902, -95.7129]
    }

    # country names
    country_names = {
        "CA": "Canada (CA)",
        "DE": "Germany (DE)",
        "FR": "France (FR)",
        "GB": "United Kingdom (GB)",
        "IN": "India (IN)",
        "JP": "Japan (JP)",
        "KR": "South Korea (KR)",
        "MX": "Mexico (MX)",
        "RU": "Russia (RU)",
        "US": "United States (US)"
    }

    # create map
    m = folium.Map(location=[20, 0], zoom_start=2)

    for country in coords:
        if country in top_tags and country in top_engagement:
            popup_html = (
                f"""
                <b>{country_names[country]}</b>
                <br><br>

                <details>
                  <summary><b>Engagement Rate ▼</b></summary>
                  <p>{top_engagement[country]}</p>
                </details>
                <br>

                <details>
                  <summary><b>Categories ▼</b></summary>
                  <p>{top_categories[country]}</p>
                </details>
                <br>

                <details>
                  <summary><b>Tags ▼</b></summary>
                  <p>{top_tags[country]}</p>
                </details>
                """
            )

            folium.Marker(
                location=coords[country],
                popup=folium.Popup(popup_html, min_width=250, max_width=250),
                tooltip=country_names[country]
            ).add_to(m)

    results_html = ""
    if search_query and search_table and search_country:
        if search_table == "engagement":
            df = eng_df[eng_df["country"] == search_country].copy()
            df = df.sort_values("engagement_rate", ascending=False)
            df["rank"] = range(1, len(df) + 1)
            filtered = df[
                df["title"].apply(lambda x: normalize(x)).str.contains(normalize(search_query))
            ]

            if not filtered.empty:
                lines = [
                    f"<b>{row['rank']}</b>. {row['title']} — {round(row['engagement_rate'], 4)}"
                    for _, row in filtered.iterrows()
                ]
                results_html = "<br>".join(lines)

        elif search_table == "categories":
            df = cat_df[cat_df["country"] == search_country].copy()
            df = df.sort_values("num_categories", ascending=False)
            df["rank"] = range(1, len(df) + 1)
            filtered = df[df["category"].str.lower().str.contains(search_query)]

            if not filtered.empty:
                lines = [
                    f"<b>{row['rank']}</b>. {row['category']} — {row['num_categories']}"
                    for _, row in filtered.iterrows()
                ]
                results_html = "<br>".join(lines)

        elif search_table == "tags":
            df = tags_df[tags_df["country"] == search_country].copy()
            df = df.sort_values("num_tags", ascending=False)
            df["rank"] = range(1, len(df) + 1)
            filtered = df[df["tag"].str.lower().str.contains(search_query)]

            if not filtered.empty:
                lines = [
                    f"<b>{row['rank']}</b>. {row['tag']} — {row['num_tags']}"
                    for _, row in filtered.iterrows()
                ]
                results_html = "<br>".join(lines)

    # dropdown
    dropdown_html = f"""
    <style>
    .dropdown-box {{
        position: fixed;
        top: 11px;
        right: 20px;
        z-index: 9999;
        background: white;
        padding: 10px;
        border-radius: 6px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        width: 260px;
    }}
    .search-box {{
        position: fixed;
        top: 66px;    
        right: 20px;
        z-index: 9999;
        background: white;
        padding: 10px;
        border-radius: 6px;
        width: 260px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
    }}
    .results-box {{
        position: fixed;
        top: 304px;
        right: 20px;
        z-index: 9999;
        background: white;
        padding: 10px;
        border-radius: 6px;
        width: 300px;
        max-height: 400px;
        overflow-y: auto;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        width: 260px;
    }}
    </style>

    <div class="dropdown-box">
        <label><b>Show Top:</b></label>
        <select onchange="window.location.href='/?top=' + this.value;">
            <option value="5" {'selected' if top_n == 5 else ''}>5</option>
            <option value="10" {'selected' if top_n == 10 else ''}>10</option>
            <option value="15" {'selected' if top_n == 15 else ''}>15</option>
        </select>
    </div>

    <div class="search-box">
        <form method="get">
            <label><b>Search:</b></label><br>
            <input type="text" name="search" value="{search_query}" style="width: 95%;">

            <br><br>

            <label><b>Type:</b></label><br>
            <select name="table" style="width: 95%;">
                <option value="">-- choose --</option>
                <option value="engagement" {'selected' if search_table=='engagement' else ''}>Engagement Rate</option>
                <option value="categories" {'selected' if search_table=='categories' else ''}>Categories</option>
                <option value="tags" {'selected' if search_table=='tags' else ''}>Tags</option>
            </select>

            <br><br>

            <label><b>Country:</b></label><br>
            <select name="country" style="width: 95%;">
            <option value="">-- choose --</option>
            {''.join([f"<option value='{c}' {'selected' if search_country==c else ''}>{country_names[c]}</option>" for c in coords])}
            </select>

            <br><br>

            <button type="submit">Search</button>
        </form>
    </div>
    """

    # search bar
    search_results_html = f"""
        <div class="results-box">
            <b>Search Results:</b>
            <br><br>
            {results_html}
        </div>
        """

    return m._repr_html_() + dropdown_html + search_results_html

# run server
if __name__ == "__main__":
    app.run(debug=True)