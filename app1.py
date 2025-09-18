import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
import traceback
import sys
import os

# Set page configuration as the first Streamlit command
st.set_page_config(
    page_title="Cotton GDD and Risk Analysis Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import Google Fonts
st.markdown(
    '<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&display=swap" rel="stylesheet">',
    unsafe_allow_html=True
)

# Ensure updated.py is in the same directory
try:
    import updated
except ImportError as e:
    st.error(f"Failed to import updated.py: {e}")
    st.error("Ensure updated.py is in the same directory as this script.")
    st.stop()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Custom CSS for enhanced UI
st.markdown("""
<style>
    /* General styling */
    .stApp {
        background-color: #f5f6f5;
        font-family: 'Manrope', sans-serif;
    }
    
    /* Main header */
    .main-header {
        font-size: 2.5rem;
        color: #1a3c34;
        text-align: center;
        margin-bottom: 1.5rem;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.6rem;
        color: #2e7d32;
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
        font-weight: 600;
    }
    
    /* Metric cards */
    .metric-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #4caf50;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        margin: 0.4rem 0;
        transition: transform 0.2s;
    }
    .metric-card:hover {
        transform: translateY(-2px);
    }
    
    /* Risk cards */
    .risk-high {
        background-color: #fff1f0;
        border-left-color: #d32f2f;
    }
    .risk-medium {
        background-color: #fff3e0;
        border-left-color: #f57c00;
    }
    .risk-low {
        background-color: #e8f5e9;
        border-left-color: #388e3c;
    }
    
    /* Phenophase cards */
    .phenophase-card {
        background-color: #e8f0fe;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1976d2;
        margin: 0.4rem 0;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

# Main title
st.markdown('<h1 class="main-header">🌱 Real time cotton crop status (Based on Heat Units approach)</h1>', unsafe_allow_html=True)

# Sidebar for user inputs
st.sidebar.markdown('<h2 class="sidebar-header">📊 Analysis Parameters</h2>', unsafe_allow_html=True)
farmer_id = st.sidebar.text_input("Farmer ID", value="518", placeholder="e.g., 10811", help="Enter the unique farmer identification number")
question_date = st.sidebar.date_input("Question Date", value=date(2025, 8, 26), help="Select the date for analysis")
get_result = st.sidebar.button("🔍 Run Analysis", type="primary")

# Function to validate inputs
def validate_inputs(farmer_id, question_date):
    try:
        farmer_id = float(farmer_id)
        if not isinstance(question_date, date):
            raise ValueError("Invalid date format. Use YYYY-MM-DD.")
        logger.debug(f"Validated inputs: Farmer ID={farmer_id}, Question Date={question_date}")
        return farmer_id, question_date
    except ValueError as e:
        st.error(f"Input error: {e}")
        logger.error(f"Input validation failed: {e}")
        return None, None

# Function to fetch farmer details
def fetch_farmer_details(farmer_id):
    try:
        logger.debug("Attempting to set up database connection")
        engine = updated.setup_database()
        logger.debug("Database connection established")
        
        # First, let's check what columns exist in the table
        try:
            # Get column information
            column_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'farmer_grid_mapping'
                ORDER BY ordinal_position
            """
            columns_df = pd.read_sql(column_query, engine)
            logger.debug(f"Available columns: {columns_df['column_name'].tolist()}")
            st.info(f"Available columns in database: {columns_df['column_name'].tolist()}")
        except Exception as col_e:
            logger.warning(f"Could not fetch column info: {col_e}")
        
        # Try the exact same query that works in updated.py
        query = """
            SELECT "Farmer ID" as farmer_id, "Farmer Name" as farmer_name, "Cluster name" as cluster_name, 
                   "Grid_ID" as grid_id, grid_lat as latitude, grid_lon as longitude, 
                   "Cotton sowing date ( કાપસાચી પેરણી ત" as sowing_date
            FROM farmer_grid_mapping 
            WHERE "Farmer ID" = %s
        """
        logger.debug(f"Executing query for Farmer ID: {farmer_id}")
        df = pd.read_sql(query, engine, params=(farmer_id,))
        if df.empty:
            st.error(f"No data found for Farmer ID: {farmer_id}")
            logger.warning(f"No data found for Farmer ID: {farmer_id}")
            return None
        logger.debug(f"Farmer data retrieved: {df.iloc[0].to_dict()}")
        return df.iloc[0]
    except Exception as e:
        st.error(f"Error fetching farmer details: {e}")
        logger.error(f"Error fetching farmer details: {traceback.format_exc()}")
        
        # If the above fails, let's try a more generic approach
        try:
            # Try to fetch all columns and let user know what's available
            query_all = "SELECT * FROM farmer_grid_mapping WHERE \"Farmer ID\" = %s LIMIT 1"
            df_all = pd.read_sql(query_all, engine, params=(farmer_id,))
            if not df_all.empty:
                st.info("Found farmer data but with different column structure:")
                st.write(df_all.columns.tolist())
                st.write(df_all.iloc[0].to_dict())
        except Exception as e2:
            st.error(f"Could not fetch farmer data at all: {e2}")
        
        return None

# Function to parse the result string into structured data
def parse_result_string(result_string):
    """Parse the string result from updated.py into structured data"""
    sections = {}
    current_section = None
    lines = result_string.split('\n')
    
    # Initialize sections
    sections['farmer_info'] = {}
    sections['analysis_info'] = {}
    sections['pbw_forecast'] = {}
    sections['insect_alerts'] = []
    sections['disease_alerts'] = []
    sections['historical_insects'] = []
    sections['historical_diseases'] = []
    sections['phenological_stages'] = []
    sections['expected_stages'] = []
    sections['harvest_prediction'] = ""
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Parse basic info
        if line.startswith('Farmer ID:'):
            sections['farmer_info']['id'] = line.split(': ')[1]
        elif line.startswith('Farmer Name:'):
            sections['farmer_info']['name'] = line.split(': ')[1]
        elif line.startswith('Cluster Name:'):
            sections['farmer_info']['cluster'] = line.split(': ')[1]
        elif line.startswith('Grid ID:'):
            sections['farmer_info']['grid_id'] = line.split(': ')[1]
        elif line.startswith('Analysis for'):
            sections['analysis_info']['location'] = line
        elif line.startswith('Current Phenophase:'):
            sections['analysis_info']['phenophase'] = line.split(': ')[1]
        elif line.startswith('Cumulative GDD:'):
            sections['analysis_info']['gdd'] = line.split(': ')[1]
        elif line.startswith('Total Rainfall since Sowing:'):
            sections['analysis_info']['rainfall'] = line.split(': ')[1]
        elif line.startswith('Average Temperature'):
            sections['analysis_info']['avg_temp'] = line.split(': ')[1]
        
        # Parse PBW section
        elif line.startswith('Pink Boll Worm (PBW) Forecast:'):
            i += 1
            while i < len(lines) and lines[i].strip() and not lines[i].startswith('Insect Risk Alerts'):
                pbw_line = lines[i].strip()
                if pbw_line.startswith('Stage:'):
                    sections['pbw_forecast']['stage'] = pbw_line.split(': ')[1]
                elif pbw_line.startswith('Generation:'):
                    sections['pbw_forecast']['generation'] = pbw_line.split(': ')[1]
                elif pbw_line.startswith('Risk Level:'):
                    sections['pbw_forecast']['risk_level'] = pbw_line.split(': ')[1]
                elif pbw_line.startswith('Weather-Based Risk Score:'):
                    sections['pbw_forecast']['risk_score'] = pbw_line.split(': ')[1]
                elif pbw_line.startswith('Management Recommendations:'):
                    sections['pbw_forecast']['recommendations'] = []
                    i += 1
                    while i < len(lines) and lines[i].strip().startswith('- '):
                        sections['pbw_forecast']['recommendations'].append(lines[i].strip()[2:])
                        i += 1
                    i -= 1  # Back up one line
                i += 1
            i -= 1  # Back up one line
        
        # Parse phenological stages
        elif line.startswith('Phenological Stage Start Dates:'):
            i += 1
            while i < len(lines) and lines[i].strip() and not lines[i].startswith('Expected'):
                stage_line = lines[i].strip()
                if stage_line and ' - ' in stage_line and ': ' in stage_line:
                    sections['phenological_stages'].append(stage_line)
                i += 1
            i -= 1
        
        # Parse expected stages
        elif line.startswith('Expected Phenophase Dates'):
            i += 1
            while i < len(lines) and lines[i].strip() and not lines[i].startswith('Crop is harvest-ready'):
                expected_line = lines[i].strip()
                if expected_line and ' - ' in expected_line and ': ' in expected_line:
                    sections['expected_stages'].append(expected_line)
                i += 1
            if i < len(lines) and lines[i].startswith('Crop is harvest-ready'):
                sections['harvest_prediction'] = lines[i]
            i -= 1
        
        # Parse harvest prediction
        elif line.startswith('Harvest Prediction') or line.startswith('Crop is harvest-ready'):
            sections['harvest_prediction'] = line
        
        i += 1
    
    return sections

# Main dashboard logic
try:
    logger.debug("Testing database connectivity")
    engine = updated.setup_database()
    st.success("✅ Database connection established successfully.")
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    logger.error(f"Database connection failed: {traceback.format_exc()}")
    st.stop()

if get_result:
    with st.spinner("Processing crop analysis..."):
        try:
            logger.debug("Processing Get Result button click")
            farmer_id_float, question_date_valid = validate_inputs(farmer_id, question_date)
            if farmer_id_float and question_date_valid:
                logger.debug(f"Fetching farmer details for ID: {farmer_id_float}")
                farmer_data = fetch_farmer_details(farmer_id_float)
                if farmer_data is not None:
                    try:
                        sowing_date = pd.to_datetime(farmer_data['sowing_date']).date()
                        lat = farmer_data['latitude']
                        lon = farmer_data['longitude']
                        grid_id = farmer_data['grid_id']
                        farmer_name = farmer_data['farmer_name']
                        cluster_name = farmer_data['cluster_name']
                        
                        logger.debug(f"Processing location: sowing_date={sowing_date}, question_date={question_date_valid}, "
                                    f"lat={lat}, lon={lon}, grid_id={grid_id}")

                        # Call the process_location function from updated.py
                        result = updated.process_location(
                            sowing_date=sowing_date,
                            question_date=question_date_valid,
                            lat=lat,
                            lon=lon,
                            grid_id=grid_id,
                            farmer_id=farmer_id_float,
                            farmer_name=farmer_name,
                            cluster_name=cluster_name,
                            insects_only=False,
                            diseases_only=False,
                            risk_threshold=40
                        )

                        # Parse the result
                        sections = parse_result_string(result)
                        
                        # Display results
                        
                        # Farmer Information Section
                        st.markdown('<h2 class="section-header">👨‍🌾 Farmer Information</h2>', unsafe_allow_html=True)
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Farmer ID", sections['farmer_info'].get('id', 'N/A'))
                        with col2:
                            st.metric("Sowing Date", sowing_date.strftime('%Y-%m-%d'))
                        st.markdown(f"**Farmer Name:** {sections['farmer_info'].get('name', 'N/A')}")
                        st.markdown(f"**Cluster:** {sections['farmer_info'].get('cluster', 'N/A')}")
                        st.markdown(f"**Grid ID:** {sections['farmer_info'].get('grid_id', 'N/A')}")

                        # Current Status Section
                        st.markdown('<h2 class="section-header">📈 Current Crop Status</h2>', unsafe_allow_html=True)
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Current Phenophase", sections['analysis_info'].get('phenophase', 'N/A'))
                        with col2:
                            st.metric("Cumulative GDD", sections['analysis_info'].get('gdd', 'N/A'))
                        with col3:
                            st.metric("Total Rainfall", sections['analysis_info'].get('rainfall', 'N/A'))
                        with col4:
                            st.metric("Avg Temperature", sections['analysis_info'].get('avg_temp', 'N/A'))

                        # Harvest Prediction
                        if sections['harvest_prediction']:
                            st.markdown('<h2 class="section-header">🚜 Harvest Status</h2>', unsafe_allow_html=True)
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #ff6b35; background-color: #fff8e1;">
                                <h3>🗓️ Status</h3>
                                <strong>{sections['harvest_prediction']}</strong>
                            </div>
                            """, unsafe_allow_html=True)

                        # Risk Assessment Section
                        st.markdown('<h2 class="section-header">⚠️ Risk Assessment</h2>', unsafe_allow_html=True)

                        # Pink Boll Worm Forecast
                        if sections['pbw_forecast']:
                            st.markdown('<h3>Pink Boll Worm (PBW) Forecast</h3>', unsafe_allow_html=True)
                            pbw = sections['pbw_forecast']
                            risk_level = pbw.get('risk_level', 'Low')
                            risk_score = pbw.get('risk_score', '0.0%')
                            risk_class = "risk-low" if risk_level == 'Low' else "risk-medium" if risk_level == 'Medium' else "risk-high"
                            
                            st.markdown(f"""
                            <div class="metric-card {risk_class}">
                                <strong>Stage:</strong> {pbw.get('stage', 'N/A')}<br>
                                <strong>Generation:</strong> {pbw.get('generation', 'N/A')}<br>
                                <strong>Risk Level:</strong> {risk_level}<br>
                                <strong>Weather Risk Score:</strong> {risk_score}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            if pbw.get('recommendations'):
                                st.markdown("**Management Recommendations:**")
                                for rec in pbw['recommendations']:
                                    st.markdown(f"• {rec}")

                        # Phenological Stages Section
                        if sections['phenological_stages'] or sections['expected_stages']:
                            st.markdown('<h2 class="section-header">🌱 Crop Development Timeline</h2>', unsafe_allow_html=True)
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown('<h3>Completed Stages</h3>', unsafe_allow_html=True)
                                if sections['phenological_stages']:
                                    for stage in sections['phenological_stages']:
                                        if ' - ' in stage and ': ' in stage:
                                            try:
                                                # Parse: "P1 - Emergence: 2025-06-13"
                                                parts = stage.split(' - ', 1)
                                                if len(parts) == 2:
                                                    stage_code = parts[0].strip()
                                                    desc_date = parts[1].rsplit(': ', 1)
                                                    if len(desc_date) == 2:
                                                        stage_desc, date_str = desc_date
                                                        st.markdown(f"""
                                                        <div class="phenophase-card">
                                                            <strong>{stage_code} - {stage_desc.strip()}</strong><br>
                                                            Date: {date_str.strip()}
                                                        </div>
                                                        """, unsafe_allow_html=True)
                                            except:
                                                st.markdown(f"<div class='phenophase-card'>{stage}</div>", unsafe_allow_html=True)
                                else:
                                    st.info("No completed stages found.")
                            
                            with col2:
                                st.markdown('<h3>Expected Future Stages</h3>', unsafe_allow_html=True)
                                if sections['expected_stages']:
                                    for stage in sections['expected_stages']:
                                        if ' - ' in stage and ': ' in stage:
                                            try:
                                                # Parse: "P8 - 50% boll open: 2025-09-15"
                                                parts = stage.split(' - ', 1)
                                                if len(parts) == 2:
                                                    stage_code = parts[0].strip()
                                                    desc_date = parts[1].rsplit(': ', 1)
                                                    if len(desc_date) == 2:
                                                        stage_desc, date_str = desc_date
                                                        st.markdown(f"""
                                                        <div class="metric-card">
                                                            <strong>{stage_code} - {stage_desc.strip()}</strong><br>
                                                            Expected: {date_str.strip()}
                                                        </div>
                                                        """, unsafe_allow_html=True)
                                            except:
                                                st.markdown(f"<div class='metric-card'>{stage}</div>", unsafe_allow_html=True)
                                else:
                                    st.info("All stages completed or no future stages predicted.")

                        # Download button for the report
                        report_filename = f"cotton_report_{farmer_id_float}_{question_date_valid.strftime('%Y%m%d')}.txt"
                        st.download_button(
                            label=f"📥 Download Full Report",
                            data=result,
                            file_name=report_filename,
                            mime="text/plain"
                        )

                    except Exception as e:
                        st.error(f"Error processing analysis: {e}")
                        logger.error(f"Error in process_location: {traceback.format_exc()}")
                        # Show raw result for debugging
                        with st.expander("Debug: Raw Result"):
                            st.text(str(e))
        except Exception as e:
            st.error(f"Unexpected error during processing: {e}")
            logger.error(f"Unexpected error: {traceback.format_exc()}")

# Sidebar information
st.sidebar.markdown("---")
st.sidebar.markdown('<h2 class="sidebar-header">ℹ️ About</h2>', unsafe_allow_html=True)
st.sidebar.info("""
This dashboard provides comprehensive cotton crop analysis including:
- Current crop status and phenophase
- Pest and disease risk assessments
- Historical risk summaries
- Phenological stage tracking
- Harvest predictions
""")

# Footer
st.markdown('<div class="footer">Cotton Crop Analysis Dashboard - Built with Streamlit 🌱</div>', unsafe_allow_html=True)