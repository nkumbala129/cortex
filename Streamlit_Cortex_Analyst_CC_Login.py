from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import requests
import snowflake.connector
import streamlit as st
import plotly.express as px

# Snowflake/Cortex Configuration
HOST = "GNB14769.snowflakecomputing.com"
DATABASE = "CORTEX_SEARCH_TUTORIAL_DB"
SCHEMA = "PUBLIC"
STAGE = "CC_STAGE"
FILE = "Climate_Career_Final_SM_Draft.yaml"

# Streamlit App Title
#st.title("Cortex Analyst")
#st.markdown(f"Semantic Model: `{FILE}`")
if "title_rendered" not in st.session_state:
    st.title("Welcome to Cortex Analyst")
    st.markdown("Please login to Interact with your Data")
    #st.markdown(f"Semantic Model: `{FILE}`")
    st.session_state.title_rendered = True


# User Authentication
if "username" not in st.session_state or "password" not in st.session_state:
    st.session_state.username = ""
    st.session_state.password = ""
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")
    if st.button("Login"):
        try:
            conn = snowflake.connector.connect(
                user=st.session_state.username,
                password=st.session_state.password,
                account="GNB14769",
                host=HOST,
                port=443,
                warehouse="CORTEX_SEARCH_TUTORIAL_WH",
                role="DEV_BR_CORTEX_AI_ROLE",
                database=DATABASE,
                schema=SCHEMA,
            )
            st.session_state.CONN = conn
            st.session_state.authenticated = True
            st.success("Authentication successful!")
            st.rerun()  # Refresh to load chat UI
        except Exception as e:
            st.error(f"Authentication failed: {e}")
else:

    def send_message(prompt: str) -> Dict[str, Any]:
        request_body = {
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        }
        resp = requests.post(
            url=f"https://{HOST}/api/v2/cortex/analyst/message",
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                "Content-Type": "application/json",
            },
        )
        request_id = resp.headers.get("X-Snowflake-Request-Id", "N/A")
        if resp.status_code < 400:
            return {**resp.json(), "request_id": request_id}
        else:
            raise Exception(f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}")
    
    def run_sql_query(sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        conn = st.session_state.CONN
        try:
            df = pd.read_sql(sql, conn)
            return df, None
        except Exception as exc:
            return None, str(exc)
    
    def display_chart_tab(df: pd.DataFrame, key_prefix: str = ""):
        if len(df.columns) < 2:
            st.write("Not enough columns to chart.")
            return
    
        all_cols = list(df.columns)
        col1, col2, col3 = st.columns(3)
        # Build unique keys using the provided key_prefix.
        x_key = f"{key_prefix}_chart_x_axis" if key_prefix else "chart_x_axis"
        y_key = f"{key_prefix}_chart_y_axis" if key_prefix else "chart_y_axis"
        type_key = f"{key_prefix}_chart_type_selector" if key_prefix else "chart_type_selector"
        
        x_col = col1.selectbox("X axis", all_cols, key=x_key)
        remaining_cols = [c for c in all_cols if c != x_col]
        y_col = col2.selectbox("Y axis", remaining_cols, key=y_key)
        chart_type = col3.selectbox(
            "Chart Type",
            ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Plot", "Area Chart"],
            key=type_key
        )
    
        # Create a copy for charting.
        chart_df = df.copy()
        if "year" in x_col.lower():
            chart_df[x_col] = chart_df[x_col].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
        if chart_type == "Line Chart":
            st.line_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart":
            st.bar_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Pie Chart":
            fig = px.pie(chart_df, names=x_col, values=y_col, title="Pie Chart")
            st.plotly_chart(fig)
        elif chart_type == "Scatter Plot":
            fig = px.scatter(chart_df, x=x_col, y=y_col, title="Scatter Plot")
            st.plotly_chart(fig)
        elif chart_type == "Area Chart":
            st.area_chart(chart_df.set_index(x_col)[y_col])
    
    def display_sql_query(sql: str):
        with st.expander("SQL Query", expanded=False):
            st.code(sql, language="sql")
        with st.expander("Results", expanded=True):
            with st.spinner("Running SQL..."):
                df, err = run_sql_query(sql)
                if err:
                    st.error(f"SQL execution error: {err}")
                    return
                if df.empty:
                    st.write("Query returned no data.")
                    return
                
                # Create a copy for display formatting.
                df_display = df.copy()
                for col in df_display.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_display[col]):
                        df_display[col] = df_display[col].dt.strftime('%Y-%m-%d')
                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                        if "year" in col.lower():
                            df_display[col] = df_display[col].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
                        else:
                            if (df_display[col] == df_display[col].astype(int)).all():
                                df_display[col] = df_display[col].astype(int)
                            else:
                                df_display[col] = df_display[col].round(2)
                
                tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
                with tab_data:
                    st.dataframe(df_display)
                with tab_chart:
                    # Derive a stable unique key prefix based on the SQL query.
                    key_prefix = f"chart_{hash(sql)}"
                    display_chart_tab(df, key_prefix=key_prefix)
    
    def display_message(content: List[Dict[str, str]], message_index: int):
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "suggestions":
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        if st.button(suggestion, key=f"suggestion_{message_index}_{suggestion_index}"):
                            st.session_state.active_suggestion = suggestion
            elif item["type"] == "sql":
                display_sql_query(item["statement"])
            else:
                st.write(f"Unsupported content type: {item['type']}")
    
    def process_message(prompt: str) -> None:
        st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating response..."):
                response = send_message(prompt=prompt)
                request_id = response["request_id"]
                content = response["message"]["content"]
                display_content(content=content, request_id=request_id)
        st.session_state.messages.append(
            {"role": "assistant", "content": content, "request_id": request_id}
        )
    
    def display_content(content: List[Dict[str, str]], request_id: Optional[str] = None, message_index: Optional[int] = None) -> None:
        message_index = message_index or len(st.session_state.messages)
        if request_id:
            with st.expander("Request ID", expanded=False):
                st.markdown(request_id)
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "suggestions":
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                            st.session_state.active_suggestion = suggestion
            elif item["type"] == "sql":
                display_sql_query(item["statement"])
            else:
                st.write(f"Unsupported content type: {item['type']}")
    
    def main():
        st.title("Cortex Analyst")
        st.markdown(f"Semantic Model: `{FILE}`")
    
        if "messages" not in st.session_state:
            st.session_state.messages = []
            st.session_state.suggestions = []
            st.session_state.active_suggestion = None
    
        for message_index, message in enumerate(st.session_state.messages):
            with st.chat_message(message["role"]):
                display_message(message["content"], message_index)
    
        user_input = st.chat_input("What is your question?")
        if user_input:
            process_message(prompt=user_input)
    
        if st.session_state.active_suggestion:
            process_message(prompt=st.session_state.active_suggestion)
            st.session_state.active_suggestion = None
    
    if __name__ == "__main__":
        main()