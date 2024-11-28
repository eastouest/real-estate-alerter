import streamlit as st
import pandas as pd
import json
import ast
from datetime import datetime
import re
from streamlit_feedback import streamlit_feedback
import pandas_gbq
import time

def clear_cache():
    st.cache_data.clear()

@st.cache_data
def load_and_process_bigquery_data(table_name, project_id="real-estate-alerter"):
    """
    Load data from BigQuery and preprocess it for filtering and feedback.
    """
    query = f"""
    SELECT 
        newsworthy_alert,
        transaction_id,
        property_description,
        property_details,
        is_newsworthy,
        feedback,
        created_date,
        JSON_EXTRACT_SCALAR(property_details, "$.document_number") AS document_number,
        CAST(JSON_EXTRACT_SCALAR(property_details, "$.transaction_sum") AS FLOAT64) AS transaction_sum,
        JSON_EXTRACT_SCALAR(property_details, "$.property_district") AS property_district,
        JSON_EXTRACT_SCALAR(property_details, "$.property_building_type_category") AS property_building_type_category,
        CAST(JSON_EXTRACT_SCALAR(property_details, "$.price_per_sqm") AS FLOAT64) AS price_per_sqm,
        CAST(JSON_EXTRACT_SCALAR(property_details, "$.property_area") AS FLOAT64) AS property_area,
        JSON_EXTRACT_SCALAR(property_details, "$.transaction_type") AS transaction_type,
        CAST(JSON_EXTRACT_SCALAR(property_details, "$.property_number_of_rooms") AS INT64) AS property_number_of_rooms,
        CAST(JSON_EXTRACT_SCALAR(property_details, "$.building_footprint") AS FLOAT64) AS building_footprint,
        JSON_EXTRACT_SCALAR(property_details, "$.built_year") AS built_year,
        JSON_EXTRACT_SCALAR(property_details, "$.is_famous") AS has_celebrity
    FROM `{project_id}.real_estate_alerter_output.{table_name}`
    """
    df = pandas_gbq.read_gbq(query, project_id=project_id)
    df['created_date'] = pd.to_datetime(df['created_date']).dt.date  # Ensure created_date is parsed as a date
    return df

def update_feedback_in_bigquery(transaction_id, feedback_label, explanation, project_id="real-estate-alerter"):
    """
    Update a single row's feedback in BigQuery using UPDATE statement
    """
    # Escape single quotes in explanation to prevent SQL injection
    escaped_explanation = explanation.replace("'", "''") if explanation else ""
    
    update_query = f"""
    UPDATE `real-estate-alerter.real_estate_alerter_output.newsworthy`
    SET 
        is_newsworthy = {feedback_label == "Newsworthy"},
        feedback = '{escaped_explanation}'
    WHERE transaction_id = '{transaction_id}'
    """
    
    try:
        pandas_gbq.read_gbq(update_query, project_id=project_id)
    except Exception as e:
        st.error(f"Update query failed: {update_query}")
        raise e

class DataProcessor:
    """Handle data processing and transformation"""
    
    @staticmethod
    def process_json_column(df, column_idx='0'):
        """Convert JSON-like string to dictionary and extract fields"""
        try:
            # Convert string representation of dict to actual dict
            df['property_data'] = df[column_idx].apply(ast.literal_eval)
            
            # Extract key fields
            extracted_fields = {
                'transaction_id': [],
                'transaction_sum': [],
                'property_district': [],
                'transaction_date': [],
                'property_number_of_rooms': [],
                'property_number_of_bathrooms': [],
                'property_area': [],
                'building_footprint': [],
                'price_per_sqm': [],
                'has_celebrity': [],
                'transaction_type': [],
                'property_building_type_category': [],
                'built_year': [],
                'document_number' : []
            }
            
            for _, row in df.iterrows():
                data = row['property_data']
                for field in extracted_fields:
                    extracted_fields[field].append(data.get(field, None))
            
            # Add extracted fields to dataframe
            for field, values in extracted_fields.items():
                df[field] = values
                
            return df
        except Exception as e:
            st.error(f"Error processing JSON data: {str(e)}")
            return df

    @staticmethod
    def clean_description(df):
        """Clean and format the property description"""
        if '1' in df.columns:
            df['description'] = df['1'].apply(lambda x: x.strip() if isinstance(x, str) else '')
        return df

    @staticmethod
    def process_alert(df):
        """Process the newsworthy alert column"""
        if '2' in df.columns:
            df['newsworthy_alert'] = df['2'].apply(lambda x: x.strip() if isinstance(x, str) else '')
        return df

def main():
    st.set_page_config(layout="wide", page_title="Real Estate Transaction Analyzer")
    
    # Initialize session state
    if 'current_tab' not in st.session_state:
        st.session_state['current_tab'] = "Overview"
    if 'selected_property' not in st.session_state:
        st.session_state['selected_property'] = None
    if 'last_clicked_index' not in st.session_state:
        st.session_state['last_clicked_index'] = None
    if 'df' not in st.session_state:
        st.session_state['df'] = None

    st.title("Real Estate Transaction Analyzer 🏠")
    
    # Sidebar: Table toggle
    st.sidebar.subheader("View Options")
    table_option = st.sidebar.radio(
        "Select table to view:",
        options=["Newsworthy", "Non-Newsworthy"]
    )
    
    # File uploader in sidebar
    with st.sidebar:
        uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
        if uploaded_file:
            try:
                # Read and process the CSV
                df = pd.read_csv(uploaded_file)
                df = DataProcessor.process_json_column(df)
                df = DataProcessor.clean_description(df)
                df = DataProcessor.process_alert(df)
                st.session_state['df'] = df
                st.success("✅ File uploaded and processed successfully!")
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

    # Determine table name based on selection
    table_name = "newsworthy" if table_option == "Newsworthy" else "non_newsworthy"

    # Load data based on selected table
    if st.session_state['df'] is None or st.session_state.get('current_table') != table_name:
        st.session_state['df'] = load_and_process_bigquery_data(table_name, project_id="real-estate-alerter")
        st.session_state['current_table'] = table_name

    df = st.session_state['df']

    # Use preloaded BigQuery data if no CSV is uploaded
    df = st.session_state.get('df')

    # Define callback for row click
    def handle_row_click(index):
        st.session_state['selected_property'] = index
        st.session_state['current_tab'] = "Detailed View"

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "🔍 Detailed View", "📈 Analytics"])

    if st.session_state['df'] is not None:
        df = st.session_state['df']
        # Preprocess DataFrame to cast 0 to "No" and any other non-zero value to "Yes" in the 'has_celebrity' column
        if 'has_celebrity' in df.columns:
            df['has_celebrity'] = df['has_celebrity'].apply(lambda x: "No" if x == '0' else x)

        with tab1:
            st.header("Transaction Overview")
            
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                district_filter = st.multiselect(
                    "Filter by District",
                    options=sorted(df['property_district'].unique())
                )
            with col2:
                type_filter = st.multiselect(
                    "Property Type",
                    options=sorted(df['property_building_type_category'].unique())
                )
            with col3:
                date_filter = st.multiselect(
                    "Date created",
                    options=sorted(df['created_date'].astype(str).unique())  # Convert to string for compatibility
                )
            with col4:
                price_range = st.slider(
                    "Price Range (NOK)",
                    min_value=int(df['transaction_sum'].min()),
                    max_value=int(df['transaction_sum'].max()),
                    value=(int(df['transaction_sum'].min()), int(df['transaction_sum'].max()))
                )

            # Apply filters
            filtered_df = df.copy()
            if district_filter:
                filtered_df = filtered_df[filtered_df['property_district'].isin(district_filter)]
            if type_filter:
                filtered_df = filtered_df[filtered_df['property_building_type_category'].isin(type_filter)]
            if date_filter:
                filtered_df = filtered_df[filtered_df['created_date'].astype(str).isin(date_filter)]  # Filter using string dates
            filtered_df = filtered_df[
                (filtered_df['transaction_sum'] >= price_range[0]) & 
                (filtered_df['transaction_sum'] <= price_range[1])
            ]

            # Display interactive dataframe with action buttons
            st.dataframe(
                filtered_df[[
                    'newsworthy_alert',
                    'property_district',
                    'has_celebrity',
                    'price_per_sqm',
                    'transaction_sum',
                    'property_building_type_category',
                    'property_area',
                    'transaction_type'
                ]].style.format({
                    'transaction_sum': '{:,.0f}',
                    'price_per_sqm': '{:,.0f}',
                    'property_area': '{:,.1f}'
                }),
                use_container_width=True,
                column_config={
                    "newsworthy_alert": st.column_config.TextColumn(
                        "Newsworthy Alert",
                        width="large",
                    ),
                    "property_district": st.column_config.TextColumn(
                        "District",
                        width="small"
                    ),
                    "has_celebrity": st.column_config.TextColumn(
                        "Celebrity",
                        width="medium"
                    ),
                    "price_per_sqm": st.column_config.NumberColumn(
                        "NOK/m²",
                        format="%d",
                        width="medium"
                    ),
                    "transaction_sum": st.column_config.NumberColumn(
                        "Price (NOK)",
                        format="%d",
                        width="medium"
                    ),
                    "property_building_type_category": st.column_config.TextColumn(
                        "Type",
                        width="medium"
                    ),
                    "property_area": st.column_config.NumberColumn(
                        "Area (m²)",
                        format="%.1f",
                        width="small"
                    ),
                    "transaction_type": st.column_config.TextColumn(
                        "Sale Type",
                        width="medium"
                    )
                }
            )
            
            # Add buttons below the table
            st.write("Select a transaction to view details:")
            cols = st.columns(4)
            for i, (idx, row) in enumerate(filtered_df.iterrows()):
                with cols[i % 4]:
                    button_label = f"{row['property_district']} - {row['transaction_sum']:,.0f} NOK"
                    if st.button(button_label, key=f"btn_{idx}"):
                        st.session_state['selected_property'] = idx
                        st.session_state['current_tab'] = "Detailed View"
                        st.rerun()

        with tab2:
            st.header("Transaction Details & Feedback")

            if st.session_state['selected_property'] is not None:
                property_data = df.iloc[st.session_state['selected_property']]
                transaction_id = property_data.get('transaction_id')  # Ensure transaction_id is defined

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Property Information")
                    st.write(f"**District:** {property_data['property_district']}")
                    st.write(f"**Type:** {property_data['property_building_type_category']}")
                    st.write(f"**Rooms:** {property_data['property_number_of_rooms']}")
                    st.write(f"**Building Footprint:** {property_data['building_footprint']} m²")
                    st.write(f"**Built Year:** {property_data['built_year']}")
                    
                with col2:
                    st.subheader("Transaction Details")
                    st.write(f"**Price:** {property_data['transaction_sum']:,.0f} NOK")
                    st.write(f"**Price per m²:** {property_data['price_per_sqm']:,.0f} NOK")
                    st.write(f"**Transaction Type:** {property_data['transaction_type']}")
                    st.write(f"**Celebrity Involved:** {'No' if property_data['has_celebrity'] == '0' else 'Yes'}")
                
                if property_data['property_description']:
                    st.subheader("Property Description")
                    
                    description = property_data['property_description']
                    
                    if "Additional details about the property" in description:
                        # Extract the section starting with "Additional details about the property"
                        import re
                        match = re.search(r"Additional details about the property: (.*)", description)
                        if match:
                            details = match.group(1)  # Capture everything after the colon
                            # Split into key-value pairs and format each pair on a new line
                            details_lines = details.split(" ")
                            formatted_details = ""
                            for line in details_lines:
                                if ":" in line:
                                    key, value = line.split(":", 1)  # Split only on the first colon
                                    formatted_details += f"**{key.strip()}**: {value.strip()}\n"
                                else:
                                    formatted_details += f"{line.strip()}\n"
                        else:
                            formatted_details = ""

                        # Render the main description without the "Additional details" section
                        main_description = description.split("Additional details about the property")[0]
                        st.write(main_description)

                        # Render the "Additional details" as a separate block
                        st.markdown(f"### Additional Details\n{formatted_details}")
                    else:
                        # If no additional details are present, render the whole description normally
                        st.write(description)


                if property_data['newsworthy_alert']:
                    st.warning(f"**Newsworthy Alert:** {property_data['newsworthy_alert']}")

                
                # Feedback section
                st.subheader("Provide Feedback")
                feedback = streamlit_feedback(feedback_type="thumbs", key=f"feedback_{st.session_state['selected_property']}")
                explanation = st.text_area("Provide a written explanation (optional):", key=f"explanation_{transaction_id}")

                if feedback:
                    # Map the feedback to a label
                    feedback_label = "Newsworthy" if feedback.get('score') == '👍' else "Not newsworthy"

                    if transaction_id:  # Ensure transaction_id exists before updating
                        try:
                            # Update BigQuery
                            update_feedback_in_bigquery(
                                transaction_id=transaction_id,
                                feedback_label=feedback_label,
                                explanation=explanation
                            )
                            st.success("✅ Feedback submitted successfully!")
                        except Exception as e:
                            st.error(f"Error submitting feedback: {str(e)}")
                    else:
                        st.error("Error: Transaction ID not found in property data.")
            else:
                st.info("Please select a property from the Overview tab.")




        with tab3:
            st.header("Market Analytics")
            district_stats = df.groupby('property_district').agg({
                'price_per_sqm': 'mean',
                'transaction_sum': 'mean',
                'property_number_of_rooms': 'mean'
            }).round(2)
            
            st.write("Average Prices by District:")
            for district, row in district_stats.iterrows():
                st.write(f"**{district}**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Avg Price/m²", f"{row['price_per_sqm']:,.0f} NOK")
                with col2:
                    st.metric("Avg Transaction", f"{row['transaction_sum']:,.0f} NOK")
                with col3:
                    st.metric("Avg Rooms", f"{row['property_number_of_rooms']:.1f}")
                st.divider()
    else:
        st.info("👆 Please upload a CSV file to get started")

if __name__ == "__main__":
    main()