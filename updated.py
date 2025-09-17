import os
import pandas as pd
import logging
import argparse
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

load_dotenv()  # loads .env into os.environ (no-op if .env not present)

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def get_db_url() -> str | None:
    """
    Return DB URL from DB_URL or build from DB_USER/DB_PASS/DB_HOST/DB_PORT/DB_NAME.
    If password contains special chars it URL-encodes it.
    """
    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url

    # fallback to components (useful if you prefer separate env vars)
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    driver = os.getenv("DB_DRIVER", "postgresql+psycopg2")

    if not (user and pwd and name):
        logging.error("DB_URL not set and/or DB_USER/DB_PASS/DB_NAME are missing.")
        return None

    pwd_quoted = quote_plus(pwd)  # encodes special chars in password
    return f"{driver}://{user}:{pwd_quoted}@{host}:{port}/{name}"


def mask_db_url(url: str) -> str:
    """Return a safe-to-log version of a DB URL with password masked."""
    try:
        u = make_url(url)
        user = u.username or "?"
        host = u.host or "?"
        port = u.port or "?"
        database = u.database or "?"
        driver = u.drivername or "?"
        return f"{driver}://{user}:***@{host}:{port}/{database}"
    except Exception:
        # fallback if parsing fails
        return "masked_db_url"


def setup_database() -> Engine:
    db_url = get_db_url()
    if not db_url:
        logging.error("No DB URL available. Set DB_URL or DB_USER/DB_PASS/DB_NAME in env.")
        raise SystemExit(1)

    logging.debug(f"Connecting using: {mask_db_url(db_url)}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        # quick test connection
        with engine.connect() as conn:
            logging.info("Database connected successfully.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Database connection failed: {e}")
        raise SystemExit(1)
# --- Cotton Phenophase Thresholds ---
phenophase_thresholds = {
    "May 20–23": {'P1': (69, 73), 'P2': (259, 279), 'P3': (316, 351), 'P4': (502, 550), 'P5': (609, 665), 'P6': (883, 962), 'P7': (1004, 1115), 'P8': (1202, 1298), 'P9': (1294, 1375), 'P10': (1369, 1502)},
    "May 24–27": {'P1': (67, 71), 'P2': (253, 272), 'P3': (308, 343), 'P4': (491, 537), 'P5': (595, 649), 'P6': (862, 938), 'P7': (981, 1087), 'P8': (1174, 1266), 'P9': (1263, 1342), 'P10': (1336, 1469)},
    "May 28–31": {'P1': (65, 69), 'P2': (246, 265), 'P3': (301, 350), 'P4': (479, 523), 'P5': (580, 633), 'P6': (841, 914), 'P7': (957, 1060), 'P8': (1146, 1235), 'P9': (1233, 1309), 'P10': (1303, 1436)},
    "June 1–4": {'P1': (64, 71), 'P2': (240, 273), 'P3': (293, 343), 'P4': (351, 351), 'P5': (364, 424), 'P6': (818, 940), 'P7': (931, 1093), 'P8': (1114, 1272), 'P9': (1199, 1346), 'P10': (1270, 1403)},
    "June 5–8": {'P1': (62, 70), 'P2': (234, 269), 'P3': (286, 337), 'P4': (455, 529), 'P5': (551, 639), 'P6': (799, 924), 'P7': (910, 1075), 'P8': (1089, 1251), 'P9': (1172, 1325), 'P10': (1237, 1370)},
    "June 9–12": {'P1': (60, 68), 'P2': (228, 264), 'P3': (279, 330), 'P4': (444, 519), 'P5': (538, 627), 'P6': (779, 908), 'P7': (887, 1057), 'P8': (1062, 1230), 'P9': (1143, 1304), 'P10': (1204, 1337)},
    "June 13–16": {'P1': (59, 66), 'P2': (222, 259), 'P3': (271, 323), 'P4': (432, 508), 'P5': (524, 615), 'P6': (759, 892), 'P7': (864, 1039), 'P8': (1035, 1209), 'P9': (1114, 1283), 'P10': (1171, 1304)},
    "June 17–20": {'P1': (56, 58), 'P2': (216, 253), 'P3': (271, 317), 'P4': (379, 443), 'P5': (410, 479), 'P6': (737, 838), 'P7': (859, 949), 'P8': (917, 1019), 'P9': (1014, 1068), 'P10': (1138, 1271)},
    "June 21–24": {'P1': (55, 57), 'P2': (210, 247), 'P3': (263, 311), 'P4': (368, 433), 'P5': (398, 467), 'P6': (716, 822), 'P7': (834, 931), 'P8': (891, 999), 'P9': (986, 1050), 'P10': (1105, 1238)},
    "June 25–28": {'P1': (53, 55), 'P2': (204, 241), 'P3': (256, 305), 'P4': (358, 423), 'P5': (387, 456), 'P6': (695, 805), 'P7': (810, 913), 'P8': (865, 979), 'P9': (957, 1031), 'P10': (1072, 1205)},
    "June 29–July 2": {'P1': (52, 54), 'P2': (198, 235), 'P3': (248, 299), 'P4': (347, 413), 'P5': (376, 444), 'P6': (675, 789), 'P7': (786, 895), 'P8': (839, 959), 'P9': (928, 1013), 'P10': (1039, 1172)},
    "July 3–6": {'P1': (50, 52), 'P2': (190, 226), 'P3': (238, 284), 'P4': (338, 396), 'P5': (364, 430), 'P6': (653, 771), 'P7': (761, 900), 'P8': (811, 961), 'P9': (895, 1008), 'P10': (1006, 1139)},
    "July 7–10": {'P1': (49, 52), 'P2': (187, 226), 'P3': (235, 284), 'P4': (333, 396), 'P5': (360, 430), 'P6': (646, 771), 'P7': (752, 900), 'P8': (802, 961), 'P9': (885, 1008), 'P10': (1000, 1106)},
    "July 11–14": {'P1': (49, 50), 'P2': (187, 220), 'P3': (235, 276), 'P4': (333, 386), 'P5': (360, 418), 'P6': (646, 752), 'P7': (752, 876), 'P8': (802, 935), 'P9': (885, 976), 'P10': (1000, 1073)},
    "July 15": {'P1': (49, 49), 'P2': (187, 214), 'P3': (235, 268), 'P4': (333, 375), 'P5': (360, 406), 'P6': (646, 732), 'P7': (752, 852), 'P8': (802, 908), 'P9': (885, 944), 'P10': (1000, 1040)},
}

# --- Phenophase Descriptions ---
phenophase_descriptions = {
    "P1": "Emergence",
    "P2": "First square",
    "P3": "First flower",
    "P4": "Peak flowering",
    "P5": "First boll",
    "P6": "Boll development",
    "P7": "First boll open",
    "P8": "50% boll open",
    "P9": "90% boll open",
    "P10": "Harvest ready"
}

# --- Insect Risks ---
insect_risks = [
    {
        "name": "Aphids",
        "phenophases": ["P1", "P2", "P3"],
        "conditions": [
            {"parameter": "temp_max", "range": [25, 35], "weight": 0.4},
            {"parameter": "humidity", "range": [60, 80], "weight": 0.3},
            {"parameter": "sunshine_hours", "range": [4, 8], "weight": 0.3}
        ],
        "threshold": 40,
        "advisory": [
            "Apply neem oil or insecticidal soap.",
            "Introduce natural predators like ladybugs.",
            "Monitor aphid populations weekly."
        ]
    },
    {
        "name": "Whitefly",
        "phenophases": ["P2", "P3", "P4"],
        "conditions": [
            {"parameter": "temp_max", "range": [28, 38], "weight": 0.5},
            {"parameter": "humidity", "range": [50, 70], "weight": 0.3},
            {"parameter": "rain_probability", "range": [0, 30], "weight": 0.2}
        ],
        "threshold": 40,
        "advisory": [
            "Use yellow sticky traps.",
            "Apply imidacloprid or thiamethoxam.",
            "Remove alternate host plants."
        ]
    }
]

# --- Disease Risks ---
disease_risks = [
    {
        "name": "Bacterial Blight",
        "phenophases": ["P2", "P3", "P4", "P5"],
        "conditions": [
            {"parameter": "temp_max", "range": [25, 35], "weight": 0.3},
            {"parameter": "humidity", "range": [70, 90], "weight": 0.4},
            {"parameter": "rain_probability", "range": [50, 100], "weight": 0.3}
        ],
        "threshold": 40,
        "advisory": [
            "Use resistant varieties.",
            "Apply copper-based bactericides.",
            "Remove and destroy infected plant debris."
        ]
    },
    {
        "name": "Fusarium Wilt",
        "phenophases": ["P3", "P4", "P5", "P6"],
        "conditions": [
            {"parameter": "temp_max", "range": [25, 32], "weight": 0.4},
            {"parameter": "humidity", "range": [60, 85], "weight": 0.3},
            {"parameter": "rainy_days", "value": 5, "weight": 0.3}
        ],
        "threshold": 40,
        "advisory": [
            "Use disease-free seeds.",
            "Apply soil fungicides.",
            "Practice crop rotation."
        ]
    }
]

# --- PBW Weather Conditions ---
pbw_weather_conditions = {
    "temp_max": {"range": [30, 40], "weight": 0.5},
    "humidity": {"range": [60, 80], "weight": 0.3},
    "rain_probability": {"range": [0, 30], "weight": 0.2}
}

# --- Fetch Weather Data ---
def fetch_weather_data(start_date, end_date, grid_id):
    try:
        engine = setup_database()
        query = """
            SELECT DATE("date") as date, grid_lat as latitude, grid_lon as longitude, temperature_2m_max AS tmax, temperature_2m_min AS tmin,
                   precipitation_sum AS precipitation, sunshine_duration / 3600 AS sunshine_hours,
                   relative_humidity_2m_mean AS humidity, cloud_cover_mean AS cloud
            FROM farmer_grid_weather_data
            WHERE DATE("date") BETWEEN %s AND %s
            AND grid_id = %s
            ORDER BY date
        """
        df = pd.read_sql(query, engine, params=(start_date, end_date, grid_id))
        df['date'] = pd.to_datetime(df['date'])
        if df.empty:
            logging.warning(f"No weather data for grid_id={grid_id} from {start_date} to {end_date}")
        return df
    except Exception as e:
        logging.error(f"Error fetching weather data: {e}")
        return pd.DataFrame()

# --- Calculate GDD ---
def calculate_gdd(tmax, tmin, tbase=15.6, tupper=34.0):
    try:
        if pd.isna(tmax) or pd.isna(tmin):
            return 0
        tmax = min(tmax, tupper)
        tmin = max(tmin, tbase)
        return ((tmax + tmin) / 2) - tbase
    except Exception as e:
        logging.error(f"Error calculating GDD: {e}")
        return 0

# --- Calculate Rainy Days ---
def calculate_rainy_days(weather_data, date):
    try:
        month_start = date.replace(day=1)
        month_data = weather_data[(weather_data['date'].dt.date >= month_start) &
                                  (weather_data['date'].dt.date <= date)]
        rainy_days = len(month_data[month_data['precipitation'] > 0])
        return rainy_days
    except Exception as e:
        logging.error(f"Error calculating rainy days: {e}")
        return 0

# --- Calculate Monthly Rainfall ---
def calculate_monthly_rainfall(weather_data, date):
    try:
        month_start = date.replace(day=1)
        month_data = weather_data[(weather_data['date'].dt.date >= month_start) &
                                  (weather_data['date'].dt.date <= date)]
        total_rainfall = month_data['precipitation'].sum()
        return total_rainfall
    except Exception as e:
        logging.error(f"Error calculating monthly rainfall: {e}")
        return 0

# --- Predict Harvest Dates ---
def predict_harvest_dates(cumulative_gdd, sowing_window, question_date):
    try:
        p10_min_gdd = phenophase_thresholds[sowing_window]['P10'][0]
        remaining_gdd = p10_min_gdd - cumulative_gdd
        if remaining_gdd <= 0:
            return ["Crop is harvest-ready (P10 reached)."]

        # Method 2: Assumed average temperature of 25°C
        assumed_avg_temp = 25.0
        assumed_gdd = max(0, assumed_avg_temp - 15.6)  # GDD = avg_temp - tbase
        days_to_harvest_25 = int(remaining_gdd / assumed_gdd)
        earliest_harvest_25 = question_date + timedelta(days=days_to_harvest_25)
        latest_harvest_25 = earliest_harvest_25 + timedelta(days=7)
        prediction_25 = f"Harvest Prediction (25°C avg temp, {assumed_gdd:.1f} GDD/day): {earliest_harvest_25.strftime('%Y-%m-%d')} to {latest_harvest_25.strftime('%Y-%m-%d')}"
        return [prediction_25]
    except Exception as e:
        logging.error(f"Error predicting harvest dates: {e}")
        return ["Harvest prediction unavailable."]

# --- Evaluate PBW Risk ---
def evaluate_pbw_risk(pbw_gdd, phenophase, weather_row, date):
    try:
        if phenophase not in ["P4", "P5", "P6", "P7", "P8"]:
            return {
                "stage": "Not active",
                "generation": "N/A",
                "adjusted_severity": "None",
                "risk_percentage": 0,
                "recommendations": ["No action required."]
            }

        stage = "Larvae" if pbw_gdd < 200 else "Pupae" if pbw_gdd < 400 else "Adult"
        generation = min(int(pbw_gdd // 600) + 1, 3)
        severity = "Low" if pbw_gdd < 300 else "Moderate" if pbw_gdd < 600 else "High"

        score = 0
        total_possible_score = sum(cond["weight"] for cond in pbw_weather_conditions.values())
        if not weather_row.empty:
            tmax = weather_row['tmax'].iloc[0] if 'tmax' in weather_row and pd.notnull(weather_row['tmax'].iloc[0]) else None
            humidity = weather_row['humidity'].iloc[0] if 'humidity' in weather_row and pd.notnull(weather_row['humidity'].iloc[0]) else None
            rain_prob = weather_row['rain_prob'].iloc[0] if 'rain_prob' in weather_row and pd.notnull(weather_row['rain_prob'].iloc[0]) else None

            for param, cond in pbw_weather_conditions.items():
                if param == "temp_max" and tmax is not None:
                    if cond["range"][0] <= tmax <= cond["range"][1]:
                        score += cond["weight"]
                elif param == "humidity" and humidity is not None:
                    if cond["range"][0] <= humidity <= cond["range"][1]:
                        score += cond["weight"]
                elif param == "rain_probability" and rain_prob is not None:
                    if cond["range"][0] <= rain_prob <= cond["range"][1]:
                        score += cond["weight"]

        risk_percentage = (score / total_possible_score) * 100 if total_possible_score > 0 else 0
        adjusted_severity = "High" if risk_percentage >= 60 and severity != "High" else severity
        recommendations = [
            "Monitor fields regularly.",
            "Use pheromone traps for PBW.",
            "Apply insecticides if larvae are detected."
        ] if risk_percentage >= 40 else ["Continue regular monitoring."]

        return {
            "stage": stage,
            "generation": f"Generation {generation}",
            "adjusted_severity": adjusted_severity,
            "risk_percentage": risk_percentage,
            "recommendations": recommendations
        }
    except Exception as e:
        logging.error(f"Error evaluating PBW risk: {e}")
        return {
            "stage": "Unknown",
            "generation": "N/A",
            "adjusted_severity": "None",
            "risk_percentage": 0,
            "recommendations": ["Error in risk evaluation."]
        }

# --- Evaluate Insect Risks ---
def evaluate_insect_risks(weather_row, phenophase, date):
    alerts = []

    try:
        if weather_row.empty:
            logging.warning("No weather data for insect risk evaluation.")
            return alerts

        tmax = weather_row['tmax'].iloc[0] if 'tmax' in weather_row and pd.notnull(weather_row['tmax'].iloc[0]) else None
        tmin = weather_row['tmin'].iloc[0] if 'tmin' in weather_row and pd.notnull(weather_row['tmin'].iloc[0]) else None
        humidity = weather_row['humidity'].iloc[0] if 'humidity' in weather_row and pd.notnull(weather_row['humidity'].iloc[0]) else None
        sunshine_hours = weather_row['sunshine_hours'].iloc[0] if 'sunshine_hours' in weather_row and pd.notnull(weather_row['sunshine_hours'].iloc[0]) else None
        rain_prob = weather_row['rain_prob'].iloc[0] if 'rain_prob' in weather_row and pd.notnull(weather_row['rain_prob'].iloc[0]) else None

        for insect in insect_risks:
            if phenophase not in insect["phenophases"]:
                continue

            score = 0
            total_possible_score = sum(cond["weight"] for cond in insect["conditions"])
            for cond in insect["conditions"]:
                param = cond["parameter"]
                weight = cond["weight"]
                if param == "temp_max" and tmax is not None:
                    if cond["range"][0] <= tmax <= cond["range"][1]:
                        score += weight
                elif param == "humidity" and humidity is not None:
                    if cond["range"][0] <= humidity <= cond["range"][1]:
                        score += weight
                elif param == "sunshine_hours" and sunshine_hours is not None:
                    if cond["range"][0] <= sunshine_hours <= cond["range"][1]:
                        score += weight
                elif param == "rain_probability" and rain_prob is not None:
                    if cond["range"][0] <= rain_prob <= cond["range"][1]:
                        score += weight

            risk_percentage = (score / total_possible_score) * 100 if total_possible_score > 0 else 0
            if risk_percentage >= insect["threshold"]:
                alerts.append({
                    "insect": insect["name"],
                    "risk_percentage": risk_percentage,
                    "advisory": insect["advisory"]
                })
        return alerts
    except Exception as e:
        logging.error(f"Error evaluating insect risks: {e}")
        return alerts

# --- Evaluate Disease Risks ---
def evaluate_disease_risks(weather_data, phenophase, date):
    alerts = []

    try:
        weather_row = weather_data[weather_data['date'].dt.date == date]
        if weather_row.empty:
            logging.warning("No weather data for disease risk evaluation.")
            return alerts

        tmax = weather_row['tmax'].iloc[0] if 'tmax' in weather_row and pd.notnull(weather_row['tmax'].iloc[0]) else None
        tmin = weather_row['tmin'].iloc[0] if 'tmin' in weather_row and pd.notnull(weather_row['tmin'].iloc[0]) else None
        humidity = weather_row['humidity'].iloc[0] if 'humidity' in weather_row and pd.notnull(weather_row['humidity'].iloc[0]) else None
        sunshine_hours = weather_row['sunshine_hours'].iloc[0] if 'sunshine_hours' in weather_row and pd.notnull(weather_row['sunshine_hours'].iloc[0]) else None
        rain_prob = weather_row['rain_prob'].iloc[0] if 'rain_prob' in weather_row and pd.notnull(weather_row['rain_prob'].iloc[0]) else None
        rainy_days = calculate_rainy_days(weather_data, date)

        for disease in disease_risks:
            if phenophase not in disease["phenophases"]:
                continue

            score = 0
            total_possible_score = sum(cond["weight"] for cond in disease["conditions"])
            for cond in disease["conditions"]:
                param = cond["parameter"]
                weight = cond["weight"]
                if param == "temp_max" and tmax is not None:
                    if cond["range"][0] <= tmax <= cond["range"][1]:
                        score += weight
                elif param == "humidity" and humidity is not None:
                    if cond["range"][0] <= humidity <= cond["range"][1]:
                        score += weight
                elif param == "rain_probability" and rain_prob is not None:
                    if cond["range"][0] <= rain_prob <= cond["range"][1]:
                        score += weight
                elif param == "rainy_days":
                    if rainy_days >= cond["value"]:
                        score += weight

            risk_percentage = (score / total_possible_score) * 100 if total_possible_score > 0 else 0
            if risk_percentage >= disease["threshold"]:
                alerts.append({
                    "disease": disease["name"],
                    "risk_percentage": risk_percentage,
                    "advisory": disease["advisory"]
                })
        return alerts
    except Exception as e:
        logging.error(f"Error evaluating disease risks: {e}")
        return alerts

# --- Determine Sowing Window ---
def determine_sowing_window(sowing_date):
    month = sowing_date.month
    day = sowing_date.day
    if month == 5:
        if 20 <= day <= 23:
            return "May 20–23"
        elif 24 <= day <= 27:
            return "May 24–27"
        elif 28 <= day <= 31:
            return "May 28–31"
    elif month == 6:
        if 1 <= day <= 4:
            return "June 1–4"
        elif 5 <= day <= 8:
            return "June 5–8"
        elif 9 <= day <= 12:
            return "June 9–12"
        elif 13 <= day <= 16:
            return "June 13–16"
        elif 17 <= day <= 20:
            return "June 17–20"
        elif 21 <= day <= 24:
            return "June 21–24"
        elif 25 <= day <= 28:
            return "June 25–28"
        elif 29 <= day <= 30:
            return "June 29–July 2"
    elif month == 7:
        if 1 <= day <= 2:
            return "June 29–July 2"
        elif 3 <= day <= 6:
            return "July 3–6"
        elif 7 <= day <= 10:
            return "July 7–10"
        elif 11 <= day <= 14:
            return "July 11–14"
        elif day == 15:
            return "July 15"
    return None

# --- Get Current Phenophase ---
def get_current_phenophase(cumulative_gdd, sowing_date):
    sowing_window = determine_sowing_window(sowing_date)
    if not sowing_window:
        return None, None
    thresholds = phenophase_thresholds[sowing_window]
    current_phenophase = "P0"  # Before emergence
    for phenophase, (min_gdd, max_gdd) in sorted(thresholds.items(), key=lambda x: x[1][0], reverse=True):
        if cumulative_gdd >= min_gdd:
            return phenophase, phenophase_descriptions[phenophase]
    return current_phenophase, "Pre-emergence"

# --- Update GDD and Track Phenophases ---
def update_gdd(sowing_date, question_date, p4_gdd_threshold, grid_id, sowing_window):
    try:
        end_date = question_date + timedelta(days=90)  # Fetch extra data for projections if needed
        weather_data = fetch_weather_data(sowing_date, end_date, grid_id)
        if weather_data.empty:
            return 0, None, 0, 0, None, weather_data, {p: None for p in phenophase_descriptions}

        cumulative_gdd = 0
        pbw_gdd = 0
        p4_start_date = None
        total_rainfall = 0
        phenophase_dates = {p: None for p in phenophase_descriptions}
        thresholds = phenophase_thresholds[sowing_window]

        for _, row in weather_data.iterrows():
            current_date = row['date'].date()
            if current_date < sowing_date:
                continue
            if current_date > question_date:
                break

            daily_gdd = calculate_gdd(row['tmax'], row['tmin'])
            cumulative_gdd += daily_gdd
            total_rainfall += row['precipitation']

            # Track phenophase start dates
            for phenophase, (min_gdd, _) in thresholds.items():
                if phenophase_dates[phenophase] is None and cumulative_gdd >= min_gdd:
                    phenophase_dates[phenophase] = current_date

            # PBW GDD starts from P4
            if p4_start_date is None and cumulative_gdd >= p4_gdd_threshold:
                p4_start_date = current_date
            if p4_start_date and current_date >= p4_start_date:
                pbw_gdd += daily_gdd

        avg_temp = (weather_data[weather_data['date'].dt.date == question_date]['tmax'].iloc[0] +
                    weather_data[weather_data['date'].dt.date == question_date]['tmin'].iloc[0]) / 2 if not weather_data.empty else None

        return cumulative_gdd, p4_start_date, pbw_gdd, total_rainfall, avg_temp, weather_data, phenophase_dates
    except Exception as e:
        logging.error(f"Error updating GDD: {e}")
        return 0, None, 0, 0, None, pd.DataFrame(), {p: None for p in phenophase_descriptions}

# --- Calculate Historical Insect Risks ---
def calculate_historical_insect_risks(sowing_date, end_date, grid_id, p4_start_date, pbw_gdd, risk_threshold=40):
    try:
        weather_data = fetch_weather_data(sowing_date, end_date, grid_id)
        if weather_data.empty:
            logging.warning(f"No weather data for historical insect risk calculation from {sowing_date} to {end_date}.")
            return f"No historical insect risks with risk score >= {risk_threshold} detected since sowing."

        insect_risk_days = {}
        insect_first_dates = {}
        insect_max_risks = {}
        insect_max_risk_dates = {}
        cumulative_gdd = 0

        for _, row in weather_data.iterrows():
            current_date = row['date'].date()
            if current_date > end_date:
                break
            daily_gdd = calculate_gdd(row['tmax'], row['tmin'])
            cumulative_gdd += daily_gdd
            phenophase, _ = get_current_phenophase(cumulative_gdd, sowing_date)
            weather_row = weather_data[weather_data['date'].dt.date == current_date]

            if phenophase:
                alerts = evaluate_insect_risks(weather_row, phenophase, current_date)
                for alert in alerts:
                    if alert['risk_percentage'] >= risk_threshold:
                        insect_name = alert['insect']
                        if insect_name not in insect_risk_days:
                            insect_risk_days[insect_name] = 0
                            insect_first_dates[insect_name] = current_date
                            insect_max_risks[insect_name] = alert['risk_percentage']
                            insect_max_risk_dates[insect_name] = current_date
                        insect_risk_days[insect_name] += 1
                        if alert['risk_percentage'] > insect_max_risks[insect_name]:
                            insect_max_risks[insect_name] = alert['risk_percentage']
                            insect_max_risk_dates[insect_name] = current_date

                if p4_start_date and current_date >= p4_start_date:
                    pbw_risk = evaluate_pbw_risk(pbw_gdd, phenophase, weather_row, current_date)
                    if pbw_risk['risk_percentage'] >= risk_threshold:
                        insect_name = "Pink Boll Worm"
                        if insect_name not in insect_risk_days:
                            insect_risk_days[insect_name] = 0
                            insect_first_dates[insect_name] = current_date
                            insect_max_risks[insect_name] = pbw_risk['risk_percentage']
                            insect_max_risk_dates[insect_name] = current_date
                        insect_risk_days[insect_name] += 1
                        if pbw_risk['risk_percentage'] > insect_max_risks.get(insect_name, 0):
                            insect_max_risks[insect_name] = pbw_risk['risk_percentage']
                            insect_max_risk_dates[insect_name] = current_date

        if not insect_risk_days:
            return f"No historical insect risks with risk score >= {risk_threshold} detected since sowing."

        summary_lines = [f"Historical Insect Risk Summary (Risk Score >= {risk_threshold} since sowing):"]
        for insect_name in sorted(insect_risk_days.keys()):
            summary_lines.append(f"{insect_name}:")
            summary_lines.append(f"  First Conducive Date: {insect_first_dates[insect_name]}")
            summary_lines.append(f"  Conducive Days: {insect_risk_days[insect_name]}")
            summary_lines.append(f"  Highest Risk: {insect_max_risks[insect_name]:.1f}% on {insect_max_risk_dates[insect_name]}")
        return "\n".join(summary_lines)
    except Exception as e:
        logging.error(f"Error calculating historical insect risks: {e}")
        return f"Error calculating historical insect risks: {e}"

# --- Calculate Historical Disease Risks ---
def calculate_historical_disease_risks(sowing_date, end_date, grid_id, risk_threshold=40):
    try:
        weather_data = fetch_weather_data(sowing_date, end_date, grid_id)
        if weather_data.empty:
            logging.warning(f"No weather data for historical disease risk calculation from {sowing_date} to {end_date}.")
            return f"No historical disease risks with risk score >= {risk_threshold} detected since sowing."

        disease_risk_days = {}
        disease_first_dates = {}
        disease_max_risks = {}
        disease_max_risk_dates = {}
        cumulative_gdd = 0

        for _, row in weather_data.iterrows():
            current_date = row['date'].date()
            if current_date > end_date:
                break
            daily_gdd = calculate_gdd(row['tmax'], row['tmin'])
            cumulative_gdd += daily_gdd
            phenophase, _ = get_current_phenophase(cumulative_gdd, sowing_date)
            if phenophase:
                disease_alerts = evaluate_disease_risks(weather_data, phenophase, current_date)
                for alert in disease_alerts:
                    if alert['risk_percentage'] >= risk_threshold:
                        disease_name = alert['disease']
                        if disease_name not in disease_risk_days:
                            disease_risk_days[disease_name] = 0
                            disease_first_dates[disease_name] = current_date
                            disease_max_risks[disease_name] = alert['risk_percentage']
                            disease_max_risk_dates[disease_name] = current_date
                        disease_risk_days[disease_name] += 1
                        if alert['risk_percentage'] > disease_max_risks[disease_name]:
                            disease_max_risks[disease_name] = alert['risk_percentage']
                            disease_max_risk_dates[disease_name] = current_date

        if not disease_risk_days:
            return f"No historical disease risks with risk score >= {risk_threshold} detected since sowing."

        summary_lines = [f"Historical Disease Risk Summary (Risk Score >= {risk_threshold} since sowing):"]
        for disease_name in sorted(disease_risk_days.keys()):
            summary_lines.append(f"{disease_name}:")
            summary_lines.append(f"  First Conducive Date: {disease_first_dates[disease_name]}")
            summary_lines.append(f"  Conducive Days: {disease_risk_days[disease_name]}")
            summary_lines.append(f"  Highest Risk: {disease_max_risks[disease_name]:.1f}% on {disease_max_risk_dates[disease_name]}")
        return "\n".join(summary_lines)
    except Exception as e:
        logging.error(f"Error calculating historical disease risks: {e}")
        return f"Error calculating historical disease risks: {e}"

# --- Process Location ---
def process_location(sowing_date, question_date, lat, lon, grid_id, farmer_id, farmer_name, cluster_name, insects_only=False, diseases_only=False, risk_threshold=40):
    try:
        sowing_date = pd.to_datetime(sowing_date).date()
        question_date = pd.to_datetime(question_date).date()
        sowing_window = determine_sowing_window(sowing_date)
        if not sowing_window:
            return f"Error: Sowing date {sowing_date} is outside valid sowing windows."

        logging.debug(f"Processing for sowing date {sowing_date}, question date {question_date}, "
                      f"grid_id={grid_id}, sowing window={sowing_window}")

        p4_gdd_threshold = phenophase_thresholds[sowing_window]['P4'][0]
        cumulative_gdd, p4_start_date, pbw_gdd, total_rainfall, avg_temp, weather_data, phenophase_dates = update_gdd(
            sowing_date, question_date, p4_gdd_threshold, grid_id, sowing_window
        )

        phenophase, phenophase_desc = get_current_phenophase(cumulative_gdd, sowing_date)
        if not phenophase:
            return f"No phenophase found for GDD {cumulative_gdd:.1f} on {question_date} at lat={lat}, lon={lon}."

        output = [f"Farmer ID: {farmer_id}", f"Farmer Name: {farmer_name}", f"Cluster Name: {cluster_name}", f"Grid ID: {grid_id}"]
        if insects_only:
            historical_insect_risks = calculate_historical_insect_risks(
                sowing_date, question_date, grid_id, p4_start_date, pbw_gdd, risk_threshold
            )
            output.append(f"Insect Risk Summary for Lat={lat}, Lon={lon}, Sowing Date={sowing_date}, Question Date={question_date}:")
            output.append(historical_insect_risks)
            return "\n".join(output)
        elif diseases_only:
            historical_disease_risks = calculate_historical_disease_risks(
                sowing_date, question_date, grid_id, risk_threshold
            )
            output.append(f"Disease Risk Summary for Lat={lat}, Lon={lon}, Sowing Date={sowing_date}, Question Date={question_date}:")
            output.append(historical_disease_risks)
            return "\n".join(output)

        weather_row = weather_data[weather_data['date'].dt.date == question_date]
        pbw_risk = evaluate_pbw_risk(pbw_gdd, phenophase, weather_row, question_date)
        insect_alerts = evaluate_insect_risks(weather_row, phenophase, question_date)
        disease_alerts = evaluate_disease_risks(weather_data, phenophase, question_date)

        historical_insect_risks = calculate_historical_insect_risks(
            sowing_date, question_date, grid_id, p4_start_date, pbw_gdd, risk_threshold
        )
        historical_disease_risks = calculate_historical_disease_risks(
            sowing_date, question_date, grid_id, risk_threshold
        )

        # Add phenophase start dates to output (only reached phenophases)
        phenophase_summary = ["Phenological Stage Start Dates:"]
        for phenophase_key, date in phenophase_dates.items():
            if date is not None:  # Only include reached phenophases
                date_str = date.strftime('%Y-%m-%d')
                phenophase_summary.append(f"  {phenophase_key} - {phenophase_descriptions[phenophase_key]}: {date_str}")

        # Estimate future phenophase dates based on 25°C average temperature
        assumed_avg_temp = 25.0
        assumed_gdd = max(0, assumed_avg_temp - 15.6)  # GDD = avg_temp - tbase = 9.4
        expected_phenophase_summary = ["Expected Phenophase Dates (based on 25°C avg temp, 9.4 GDD/day):"]
        thresholds = phenophase_thresholds[sowing_window]
        for phenophase_key, (min_gdd, max_gdd) in thresholds.items():
            if phenophase_dates[phenophase_key] is None:  # Only for unachieved phenophases
                median_gdd = (min_gdd + max_gdd) / 2
                remaining_gdd = median_gdd - cumulative_gdd
                if remaining_gdd > 0:
                    days_to_reach = int(remaining_gdd / assumed_gdd) + 1  # Round up to ensure reaching median
                    expected_date = question_date + timedelta(days=days_to_reach)
                    expected_phenophase_summary.append(
                        f"  {phenophase_key} - {phenophase_descriptions[phenophase_key]}: {expected_date.strftime('%Y-%m-%d')}"
                    )

        output.extend([
            f"Analysis for Lat={lat}, Lon={lon}, Sowing Date={sowing_date}, Question Date={question_date}",
            f"Current Phenophase: {phenophase} - {phenophase_desc}",
            f"Cumulative GDD: {cumulative_gdd:.1f}",
            f"Total Rainfall since Sowing: {total_rainfall:.1f} mm",
            f"Average Temperature on {question_date}: {avg_temp:.1f}°C" if avg_temp else "Average Temperature: N/A",
            "\nPink Boll Worm (PBW) Forecast:",
            f"  Stage: {pbw_risk['stage']}",
            f"  Generation: {pbw_risk['generation']}",
            f"  Risk Level: {pbw_risk['adjusted_severity']}",
            f"  Weather-Based Risk Score: {pbw_risk['risk_percentage']:.1f}%",
            "  Management Recommendations:",
            *[f"    - {rec}" for rec in pbw_risk['recommendations']],
            "\nInsect Risk Alerts (Excluding PBW):",
        ])

        if insect_alerts:
            for alert in insect_alerts:
                output.extend([
                    f"  {alert['insect']} (Risk Score: {alert['risk_percentage']:.1f}%):",
                    *[f"    - {adv}" for adv in alert['advisory']],
                ])
        else:
            output.append("  No significant insect risks detected today.")

        output.extend([
            "\nDisease Risk Alerts:",
        ])

        if disease_alerts:
            for alert in disease_alerts:
                output.extend([
                    f"  {alert['disease']} (Risk Score: {alert['risk_percentage']:.1f}%):",
                    *[f"    - {adv}" for adv in alert['advisory']],
                ])
        else:
            output.append("  No significant disease risks detected today.")

        output.extend([
            "\nHistorical Summaries:",
            historical_insect_risks,
            "\n",
            historical_disease_risks,
            "\n"
        ])

        output.extend(phenophase_summary)
        output.extend(["\n"])
        output.extend(expected_phenophase_summary)
        output.extend(["\n"])
        output.extend(predict_harvest_dates(cumulative_gdd, sowing_window, question_date))

        return "\n".join(output)
    except Exception as e:
        logging.error(f"Error processing grid_id={grid_id}: {e}")
        return f"Error processing grid_id={grid_id}: {e}"

# --- Interactive Input Handler ---
# --- Interactive Input Handler ---
# --- Interactive Input Handler ---
def get_user_inputs():
    def valid_date(s):
        # Check if the input is already a datetime.date object
        from datetime import datetime, date
        if isinstance(s, date):  # Use date directly from datetime module
            return s
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD.")

    parser = argparse.ArgumentParser(description="Cotton GDD and Risk Analysis")
    parser.add_argument("--farmer-id", type=float, help="Farmer ID")
    parser.add_argument("--question-date", type=valid_date, help="Question date (YYYY-MM-DD)")
    parser.add_argument("--insects-only", action="store_true", help="Show only insect risk summary")
    parser.add_argument("--diseases-only", action="store_true", help="Show only disease risk summary")
    parser.add_argument("--risk-threshold", type=float, default=40, help="Risk threshold percentage (default: 40)")

    args = parser.parse_args()

    engine = setup_database()

    # Interactive prompts if arguments are not provided
    if args.farmer_id is None:
        while True:
            try:
                farmer_id_str = input("Enter Farmer ID: ")
                farmer_id = float(farmer_id_str)
                break
            except ValueError:
                print("Invalid input. Enter a number.")
    else:
        farmer_id = args.farmer_id

    query = """
        SELECT "Farmer ID" as farmer_id, "Farmer Name" as farmer_name, "Cluster name" as cluster_name, 
               "Grid_ID" as grid_id, grid_lat as latitude, grid_lon as longitude, 
               "Cotton sowing date ( कापसाची पेरणी त" as sowing_date
        FROM farmer_grid_mapping 
        WHERE "Farmer ID" = %s
    """
    df = pd.read_sql(query, engine, params=(farmer_id,))
    if df.empty:
        print(f"No data found for Farmer ID: {farmer_id}")
        exit(1)

    farmer_name = df['farmer_name'].iloc[0]
    cluster_name = df['cluster_name'].iloc[0]
    grid_id = df['grid_id'].iloc[0]
    lat = df['latitude'].iloc[0]
    lon = df['longitude'].iloc[0]
    sowing_date = df['sowing_date'].iloc[0]

    # Handle sowing_date
    try:
        sowing_date = valid_date(sowing_date)
    except ValueError as e:
        print(f"Invalid sowing date in database: {sowing_date}")
        exit(1)

    if args.question_date is None:
        while True:
            try:
                question_date_str = input("Enter question date (YYYY-MM-DD): ")
                question_date = valid_date(question_date_str)
                break
            except ValueError as e:
                print(e)
    else:
        question_date = args.question_date

    insects_only = args.insects_only
    diseases_only = args.diseases_only
    risk_threshold = args.risk_threshold

    if insects_only and diseases_only:
        print("Error: Cannot select both --insects-only and --diseases-only.")
        exit(1)

    return sowing_date, question_date, lat, lon, grid_id, farmer_id, farmer_name, cluster_name, insects_only, diseases_only, risk_threshold

# --- Main Execution ---
def main():
    try:
        setup_database()
        sowing_date, question_date, lat, lon, grid_id, farmer_id, farmer_name, cluster_name, insects_only, diseases_only, risk_threshold = get_user_inputs()
        result = process_location(sowing_date, question_date, lat, lon, grid_id, farmer_id, farmer_name, cluster_name, insects_only, diseases_only, risk_threshold)
        print(result)
        print("=" * 80)
    except Exception as e:
        logging.error(f"Main execution failed: {e}")

if __name__ == "__main__":
    main()