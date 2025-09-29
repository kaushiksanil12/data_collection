# import requests
# import pandas as pd
# import json
# import time
# import logging
# from datetime import datetime
# import concurrent.futures
# from tqdm import tqdm
# import oracledb
# from sqlalchemy import create_engine, text
# from datetime import datetime, timedelta
# from sqlalchemy.types import VARCHAR, DECIMAL, TIMESTAMP, INTEGER
# import os
from datetime import datetime, timedelta
import concurrent.futures
import logging
import pandas as pd
from sqlalchemy import create_engine, text
import requests
from tqdm import tqdm

# Complete STATE_DISTRICT_MAPPING for all India states
STATE_DISTRICT_MAPPING = {
    'Andaman and Nicobar Islands': ['Nicobar', 'North and Middle Andaman', 'South Andaman'],
    'Andhra Pradesh': [
        'Alluri Sitharama Raju', 'Anakapalli', 'Anantapur', 'Annamayya', 'Bapatla', 
        'Chittoor', 'Dr. B.R. Ambedkar Konaseema', 'East Godavari', 'Eluru', 'Guntur', 
        'Kakinada', 'Krishna', 'Kurnool', 'Nandyal', 'NTR', 'Palnadu', 
        'Parvathipuram Manyam', 'Prakasam', 'Sri Potti Sriramulu Nellore', 
        'Sri Sathya Sai', 'Srikakulam', 'Tirupati', 'Visakhapatnam', 'Vizianagaram', 
        'West Godavari', 'YSR Kadapa'
    ],
    'Arunachal Pradesh': [
        'Anjaw', 'Changlang', 'Dibang Valley', 'East Kameng', 'East Siang', 
        'Kamle', 'Kra Daadi', 'Kurung Kumey', 'Lepa Rada', 'Lohit', 'Longding', 
        'Lower Dibang Valley', 'Lower Siang', 'Lower Subansiri', 'Namsai', 
        'Pakke Kessang', 'Papum Pare', 'Shi Yomi', 'Siang', 'Tawang', 'Tirap', 
        'Upper Dibang Valley', 'Upper Siang', 'Upper Subansiri', 'West Kameng', 'West Siang'
    ],
    'Assam': [
        'Bajali', 'Baksa', 'Barpeta', 'Biswanath', 'Bongaigaon', 'Cachar', 
        'Charaideo', 'Chirang', 'Darrang', 'Dhemaji', 'Dhubri', 'Dibrugarh', 
        'Dima Hasao', 'Goalpara', 'Golaghat', 'Hailakandi', 'Hojai', 'Jorhat', 
        'Kamrup', 'Kamrup Metropolitan', 'Karbi Anglong', 'Karimganj', 'Kokrajhar', 
        'Lakhimpur', 'Majuli', 'Morigaon', 'Nagaon', 'Nalbari', 'Sivasagar', 
        'Sonitpur', 'South Salmara-Mankachar', 'Tamulpur', 'Tinsukia', 'Udalguri', 
        'West Karbi Anglong'
    ],
    'Bihar': [
        'Araria', 'Arwal', 'Aurangabad', 'Banka', 'Begusarai', 'Bhagalpur', 
        'Bhojpur', 'Buxar', 'Darbhanga', 'East Champaran', 'Gaya', 'Gopalganj', 
        'Jamui', 'Jehanabad', 'Kaimur', 'Katihar', 'Khagaria', 'Kishanganj', 
        'Lakhisarai', 'Madhepura', 'Madhubani', 'Munger', 'Muzaffarpur', 'Nalanda', 
        'Nawada', 'Patna', 'Purnia', 'Rohtas', 'Saharsa', 'Samastipur', 'Saran', 
        'Sheikhpura', 'Sheohar', 'Sitamarhi', 'Siwan', 'Supaul', 'Vaishali', 
        'West Champaran'
    ],
    'Chandigarh': ['Chandigarh'],
    'Chhattisgarh': [
        'Balod', 'Baloda Bazar', 'Balrampur', 'Bastar', 'Bemetara', 'Bijapur', 
        'Bilaspur', 'Dantewada', 'Dhamtari', 'Durg', 'Gariaband', 'Gaurela Pendra Marwahi', 
        'Janjgir-Champa', 'Jashpur', 'Kabirdham', 'Kanker', 'Khairagarh-Chhuikhadan-Gandai', 
        'Kondagaon', 'Korba', 'Koriya', 'Mahasamund', 'Manendragarh-Chirmiri-Bharatpur', 
        'Mohla-Manpur-Ambagarh Chowki', 'Mungeli', 'Narayanpur', 'Raigarh', 
        'Raipur', 'Rajnandgaon', 'Sarangarh-Bilaigarh', 'Shakti', 'Sukma', 
        'Surajpur', 'Surguja'
    ],
    'Dadra and Nagar Haveli and Daman and Diu': ['Daman', 'Diu', 'Dadra and Nagar Haveli'],
    'Delhi': [
        'Central Delhi', 'East Delhi', 'New Delhi', 'North Delhi', 'North East Delhi', 
        'North West Delhi', 'Shahdara', 'South Delhi', 'South East Delhi', 
        'South West Delhi', 'West Delhi'
    ],
    'Goa': ['North Goa', 'South Goa'],
    'Gujarat': [
        'Ahmedabad', 'Amreli', 'Anand', 'Aravalli', 'Banaskantha', 'Bharuch', 
        'Bhavnagar', 'Botad', 'Chhota Udaipur', 'Dahod', 'Dang', 'Devbhoomi Dwarka', 
        'Gandhinagar', 'Gir Somnath', 'Jamnagar', 'Junagadh', 'Kheda', 'Kutch', 
        'Mahisagar', 'Mehsana', 'Morbi', 'Narmada', 'Navsari', 'Panchmahal', 
        'Patan', 'Porbandar', 'Rajkot', 'Sabarkantha', 'Surat', 'Surendranagar', 
        'Tapi', 'Vadodara', 'Valsad'
    ],
    'Haryana': [
        'Ambala', 'Bhiwani', 'Charkhi Dadri', 'Faridabad', 'Fatehabad', 'Gurugram', 
        'Hisar', 'Jhajjar', 'Jind', 'Kaithal', 'Karnal', 'Kurukshetra', 'Mahendragarh', 
        'Nuh', 'Palwal', 'Panchkula', 'Panipat', 'Rewari', 'Rohtak', 'Sirsa', 
        'Sonipat', 'Yamunanagar'
    ],
    'Himachal Pradesh': [
        'Bilaspur', 'Chamba', 'Hamirpur', 'Kangra', 'Kinnaur', 'Kullu', 
        'Lahaul and Spiti', 'Mandi', 'Shimla', 'Sirmaur', 'Solan', 'Una'
    ],
    'Jammu and Kashmir': [
        'Anantnag', 'Bandipora', 'Baramulla', 'Budgam', 'Doda', 'Ganderbal', 
        'Jammu', 'Kathua', 'Kishtwar', 'Kulgam', 'Kupwara', 'Poonch', 'Pulwama', 
        'Rajouri', 'Ramban', 'Reasi', 'Samba', 'Shopian', 'Srinagar', 'Udhampur'
    ],
    'Jharkhand': [
        'Bokaro', 'Chatra', 'Deoghar', 'Dhanbad', 'Dumka', 'East Singhbhum', 
        'Garhwa', 'Giridih', 'Godda', 'Gumla', 'Hazaribagh', 'Jamtara', 'Khunti', 
        'Koderma', 'Latehar', 'Lohardaga', 'Pakur', 'Palamu', 'Ramgarh', 'Ranchi', 
        'Sahebganj', 'Seraikela Kharsawan', 'Simdega', 'West Singhbhum'
    ],
    'Karnataka': [
        'Bagalkot', 'Ballari', 'Belagavi', 'Bengaluru Rural', 'Bengaluru Urban', 
        'Bidar', 'Chamarajanagar', 'Chikballapur', 'Chikkamagaluru', 'Chitradurga', 
        'Dakshina Kannada', 'Davanagere', 'Dharwad', 'Gadag', 'Hassan', 'Haveri', 
        'Kalaburagi', 'Kodagu', 'Kolar', 'Koppal', 'Mandya', 'Mysuru', 'Raichur', 
        'Ramanagara', 'Shivamogga', 'Tumakuru', 'Udupi', 'Uttara Kannada', 
        'Vijayanagara', 'Vijayapura', 'Yadgir'
    ],
    'Kerala': [
        'Alappuzha', 'Ernakulam', 'Idukki', 'Kannur', 'Kasaragod', 'Kollam', 
        'Kottayam', 'Kozhikode', 'Malappuram', 'Palakkad', 'Pathanamthitta', 
        'Thiruvananthapuram', 'Thrissur', 'Wayanad'
    ],
    'Ladakh': ['Kargil', 'Leh'],
    'Lakshadweep': ['Lakshadweep'],
    'Madhya Pradesh': [
        'Agar Malwa', 'Alirajpur', 'Anuppur', 'Ashoknagar', 'Balaghat', 'Barwani', 
        'Betul', 'Bhind', 'Bhopal', 'Burhanpur', 'Chhatarpur', 'Chhindwara', 
        'Damoh', 'Datia', 'Dewas', 'Dhar', 'Dindori', 'Guna', 'Gwalior', 'Harda', 
        'Hoshangabad', 'Indore', 'Jabalpur', 'Jhabua', 'Katni', 'Khandwa', 
        'Khargone', 'Mandla', 'Mandsaur', 'Morena', 'Narsinghpur', 'Neemuch', 
        'Niwari', 'Panna', 'Raisen', 'Rajgarh', 'Ratlam', 'Rewa', 'Sagar', 'Satna', 
        'Sehore', 'Seoni', 'Shahdol', 'Shajapur', 'Sheopur', 'Shivpuri', 'Sidhi', 
        'Singrauli', 'Tikamgarh', 'Ujjain', 'Umaria', 'Vidisha'
    ],
    'Maharashtra': [
        'Ahmednagar', 'Akola', 'Amravati', 'Aurangabad', 'Beed', 'Bhandara', 
        'Buldhana', 'Chandrapur', 'Dhule', 'Gadchiroli', 'Gondia', 'Hingoli', 
        'Jalgaon', 'Jalna', 'Kolhapur', 'Latur', 'Mumbai City', 'Mumbai Suburban', 
        'Nagpur', 'Nanded', 'Nandurbar', 'Nashik', 'Osmanabad', 'Palghar', 
        'Parbhani', 'Pune', 'Raigad', 'Ratnagiri', 'Sangli', 'Satara', 'Sindhudurg', 
        'Solapur', 'Thane', 'Wardha', 'Washim', 'Yavatmal'
    ],
    'Manipur': [
        'Bishnupur', 'Chandel', 'Churachandpur', 'Imphal East', 'Imphal West', 
        'Jiribam', 'Kakching', 'Kamjong', 'Kangpokpi', 'Noney', 'Pherzawl', 
        'Senapati', 'Tamenglong', 'Tengnoupal', 'Thoubal', 'Ukhrul'
    ],
    'Meghalaya': [
        'East Garo Hills', 'East Jaintia Hills', 'East Khasi Hills', 'North Garo Hills', 
        'Ri Bhoi', 'South Garo Hills', 'South West Garo Hills', 'South West Khasi Hills', 
        'West Garo Hills', 'West Jaintia Hills', 'West Khasi Hills', 'Eastern West Khasi Hills'
    ],
    'Mizoram': [
        'Aizawl', 'Champhai', 'Hnahthial', 'Khawzawl', 'Kolasib', 'Lawngtlai', 
        'Lunglei', 'Mamit', 'Saiha', 'Saitual', 'Serchhip'
    ],
    'Nagaland': [
        'Chümoukedima', 'Dimapur', 'Kiphire', 'Kohima', 'Longleng', 'Mokokchung', 
        'Mon', 'Niuland', 'Noklak', 'Peren', 'Phek', 'Shamator', 'Tseminyü', 
        'Tuensang', 'Wokha', 'Zunheboto'
    ],
    'Odisha': [
        'Angul', 'Balangir', 'Balasore', 'Bargarh', 'Bhadrak', 'Boudh', 'Cuttack', 
        'Deogarh', 'Dhenkanal', 'Gajapati', 'Ganjam', 'Jagatsinghpur', 'Jajpur', 
        'Jharsuguda', 'Kalahandi', 'Kandhamal', 'Kendrapara', 'Kendujhar', 
        'Khordha', 'Koraput', 'Malkangiri', 'Mayurbhanj', 'Nabarangpur', 'Nayagarh', 
        'Nuapada', 'Puri', 'Rayagada', 'Sambalpur', 'Subarnapur', 'Sundargarh'
    ],
    'Puducherry': ['Karaikal', 'Mahe', 'Puducherry', 'Yanam'],
    'Punjab': [
        'Amritsar', 'Barnala', 'Bathinda', 'Faridkot', 'Fatehgarh Sahib', 'Fazilka', 
        'Ferozepur', 'Gurdaspur', 'Hoshiarpur', 'Jalandhar', 'Kapurthala', 
        'Ludhiana', 'Malerkotla', 'Mansa', 'Moga', 'Muktsar', 'Pathankot', 'Patiala', 
        'Rupnagar', 'Sahibzada Ajit Singh Nagar', 'Sangrur', 'Shaheed Bhagat Singh Nagar', 
        'Sri Muktsar Sahib', 'Tarn Taran'
    ],
    'Rajasthan': [
        'Ajmer', 'Alwar', 'Anupgarh', 'Balotra', 'Banswara', 'Baran', 'Barmer', 
        'Beawar', 'Bharatpur', 'Bhilwara', 'Bikaner', 'Bundi', 'Chittorgarh', 
        'Churu', 'Dausa', 'Deeg', 'Dholpur', 'Didwana-Kuchaman', 'Dudu', 'Dungarpur', 
        'Ganganagar', 'Gangapur City', 'Hanumangarh', 'Jaipur', 'Jaipur Rural', 
        'Jaisalmer', 'Jalore', 'Jhalawar', 'Jhunjhunu', 'Jodhpur', 'Jodhpur Rural', 
        'Karauli', 'Kekri', 'Khairthal-Tijara', 'Kota', 'Kotputli-Behror', 'Nagaur', 
        'Pali', 'Phalodi', 'Pratapgarh', 'Rajsamand', 'Salumbar', 'Sanchore', 
        'Sawai Madhopur', 'Shahpura', 'Sikar', 'Sirohi', 'Tonk', 'Udaipur'
    ],
    'Sikkim': ['East Sikkim', 'North Sikkim', 'Pakyong', 'Soreng', 'South Sikkim', 'West Sikkim'],
    'Tamil Nadu': [
        'Ariyalur', 'Chengalpattu', 'Chennai', 'Coimbatore', 'Cuddalore', 'Dharmapuri', 
        'Dindigul', 'Erode', 'Kallakurichi', 'Kancheepuram', 'Kanyakumari', 'Karur', 
        'Krishnagiri', 'Madurai', 'Mayiladuthurai', 'Nagapattinam', 'Namakkal', 
        'Nilgiris', 'Perambalur', 'Pudukkottai', 'Ramanathapuram', 'Ranipet', 
        'Salem', 'Sivaganga', 'Tenkasi', 'Thanjavur', 'Theni', 'Thoothukudi', 
        'Tiruchirappalli', 'Tirunelveli', 'Tirupathur', 'Tiruppur', 'Tiruvallur', 
        'Tiruvannamalai', 'Tiruvarur', 'Vellore', 'Viluppuram', 'Virudhunagar'
    ],
    'Telangana': [
        'Adilabad', 'Bhadradri Kothagudem', 'Hyderabad', 'Jagtial', 'Jangaon', 
        'Jayashankar Bhupalpally', 'Jogulamba Gadwal', 'Kamareddy', 'Karimnagar', 
        'Khammam', 'Komaram Bheem', 'Mahabubabad', 'Mahbubnagar', 'Mancherial', 
        'Medak', 'Medchal-Malkajgiri', 'Mulugu', 'Nagarkurnool', 'Nalgonda', 
        'Narayanpet', 'Nirmal', 'Nizamabad', 'Peddapalli', 'Rajanna Sircilla', 
        'Ranga Reddy', 'Sangareddy', 'Siddipet', 'Suryapet', 'Vikarabad', 'Wanaparthy', 
        'Warangal', 'Hanamkonda', 'Yadadri Bhuvanagiri'
    ],
    'Tripura': [
        'Dhalai', 'Gomati', 'Khowai', 'North Tripura', 'Sepahijala', 'South Tripura', 
        'Unakoti', 'West Tripura'
    ],
    'Uttar Pradesh': [
        'Agra', 'Aligarh', 'Ambedkar Nagar', 'Amethi', 'Amroha', 'Auraiya', 'Ayodhya', 
        'Azamgarh', 'Baghpat', 'Bahraich', 'Ballia', 'Balrampur', 'Banda', 
        'Barabanki', 'Bareilly', 'Basti', 'Bhadohi', 'Bijnor', 'Budaun', 
        'Bulandshahr', 'Chandauli', 'Chitrakoot', 'Deoria', 'Etah', 'Etawah', 
        'Farrukhabad', 'Fatehpur', 'Firozabad', 'Gautam Buddha Nagar', 'Ghaziabad', 
        'Ghazipur', 'Gonda', 'Gorakhpur', 'Hamirpur', 'Hapur', 'Hardoi', 'Hathras', 
        'Jalaun', 'Jaunpur', 'Jhansi', 'Kannauj', 'Kanpur Dehat', 'Kanpur Nagar', 
        'Kasganj', 'Kaushambi', 'Kheri', 'Kushinagar', 'Lalitpur', 'Lucknow', 
        'Maharajganj', 'Mahoba', 'Mainpuri', 'Mathura', 'Mau', 'Meerut', 'Mirzapur', 
        'Moradabad', 'Muzaffarnagar', 'Pilibhit', 'Pratapgarh', 'Prayagraj', 'Raebareli', 
        'Rampur', 'Saharanpur', 'Sambhal', 'Sant Kabir Nagar', 'Shahjahanpur', 'Shamli', 
        'Shravasti', 'Siddharthnagar', 'Sitapur', 'Sonbhadra', 'Sultanpur', 'Unnao', 'Varanasi'
    ],
    'Uttarakhand': [
        'Almora', 'Bageshwar', 'Chamoli', 'Champawat', 'Dehradun', 'Haridwar', 
        'Nainital', 'Pauri Garhwal', 'Pithoragarh', 'Rudraprayag', 'Tehri Garhwal', 
        'Udham Singh Nagar', 'Uttarkashi'
    ],
    'West Bengal': [
        'Alipurduar', 'Bankura', 'Birbhum', 'Cooch Behar', 'Dakshin Dinajpur', 
        'Darjeeling', 'Hooghly', 'Howrah', 'Jalpaiguri', 'Jhargram', 'Kalimpong', 
        'Kolkata', 'Malda', 'Murshidabad', 'Nadia', 'North 24 Parganas', 
        'Paschim Bardhaman', 'Paschim Medinipur', 'Purba Bardhaman', 'Purba Medinipur', 
        'Purulia', 'South 24 Parganas', 'Uttar Dinajpur'
    ]
}


class OracleETLGroundwaterCollector:
    def __init__(self, oracle_config, target_date='2025-01-01'):
        self.base_url = "https://indiawris.gov.in/Dataset/Ground Water Level"
        self.session = requests.Session()
        self.session.headers.update({'accept': 'application/json'})
        self.target_date = target_date
        self.oracle_config = oracle_config
        self.engine = None
        self.stats = {
            'total_districts': 0,
            'successful_districts': 0,
            'failed_districts': 0,
            'total_raw_records': 0,
            'total_cleaned_records': 0,
            'duplicates_removed': 0,
            'api_retries': 0,
            'db_insert_batches': 0,
            'db_total_inserted': 0,
            'records_deleted': 0
        }
        self.columns_to_remove = [
            'fetch_timestamp', 'source_district', 'source_state',
            'village', 'block', 'description', 'tributary', 'majorBasin'
        ]
        self.table_name = 'GROUNDWATER_MONITORING'
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def setup_oracle_connection(self):
        try:
            oracle_url = f"oracle+oracledb://{self.oracle_config['username']}:{self.oracle_config['password']}@" \
                         f"{self.oracle_config['host']}:{self.oracle_config['port']}/{self.oracle_config['service_name']}"
            self.engine = create_engine(
                oracle_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 'Oracle 21c Connected!' as status FROM DUAL"))
                status = result.fetchone()[0]
                print(f"✅ {status}")

                pdb_result = conn.execute(text("SELECT SYS_CONTEXT('USERENV','CON_NAME') as pdb_name FROM DUAL"))
                pdb_name = pdb_result.fetchone()[0]
                print(f"✅ Connected to: {pdb_name}")
            return True
        except Exception as e:
            self.logger.error(f"Oracle connection failed: {e}")
            return False

    def delete_by_date(self, target_date):
        """Delete data for a specific date"""
        try:
            delete_sql = f"""
            DELETE FROM {self.table_name} 
            WHERE TRUNC(DATA_TIME) = TRUNC(TO_DATE(:target_date, 'YYYY-MM-DD'))
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(delete_sql), {"target_date": target_date})
                rows_deleted = result.rowcount
                conn.commit()
            print(f"✅ Deleted {rows_deleted:,} records for date {target_date}")
            self.logger.info(f"Deleted {rows_deleted} records for date {target_date}")
            self.stats['records_deleted'] = rows_deleted
            return rows_deleted
        except Exception as e:
            self.logger.error(f"Failed to delete data for date {target_date}: {e}")
            print(f"❌ Failed to delete data for date {target_date}: {e}")
            return 0

    def extract_and_transform_district(self, state_name, district_name):
        params = {
            'stateName': state_name,
            'districtName': district_name,
            'agencyName': 'CGWB',
            'startdate': self.target_date,
            'enddate': self.target_date,
            'download': 'false',
            'page': 0,
            'size': 1000
        }
        max_retries = 2
        attempt = 0
        while attempt <= max_retries:
            try:
                response = self.session.post(self.base_url, params=params, data='', timeout=45)
                if response.status_code == 200:
                    data = response.json()
                    raw_records = []
                    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                        raw_records = data['data']
                    if not raw_records:
                        return ('success', 0, 0, [])
                    for record in raw_records:
                        if isinstance(record, dict):
                            record['source_state'] = state_name
                            record['source_district'] = district_name
                            record['fetch_timestamp'] = datetime.now().isoformat()
                    df_raw = pd.DataFrame(raw_records)
                    df_cleaned = self.transform_data(df_raw)
                    cleaned_records = df_cleaned.to_dict('records') if not df_cleaned.empty else []
                    return ('success', len(raw_records), len(cleaned_records), cleaned_records)
                else:
                    attempt += 1
                    if attempt <= max_retries:
                        self.stats['api_retries'] += 1
                        self.logger.warning(f"HTTP {response.status_code} for {state_name}/{district_name}, retrying ({attempt}/{max_retries})")
                        time.sleep(1)
                    else:
                        self.logger.error(f"Max retries exceeded for {state_name}/{district_name}")
                        return ('http_error', 0, 0, [])
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt <= max_retries:
                    self.stats['api_retries'] += 1
                    self.logger.warning(f"Request exception for {state_name}/{district_name}: {e}, retrying ({attempt}/{max_retries})")
                    time.sleep(1)
                else:
                    self.logger.error(f"Max retries exceeded for {state_name}/{district_name}: {e}")
                    return ('exception', 0, 0, [])
        return ('exception', 0, 0, [])

    def transform_data(self, df_raw):
        if df_raw.empty:
            return df_raw
        original_count = len(df_raw)
        existing_columns_to_remove = [col for col in self.columns_to_remove if col in df_raw.columns]
        df_cleaned = df_raw.drop(columns=existing_columns_to_remove, errors='ignore')
        if 'dataTime' in df_cleaned.columns:
            df_cleaned['dataTime'] = pd.to_datetime(df_cleaned['dataTime'], errors='coerce')
        if all(col in df_cleaned.columns for col in ['stationCode', 'dataTime']):
            before_dedup = len(df_cleaned)
            df_cleaned = df_cleaned.drop_duplicates(subset=['stationCode', 'dataTime'], keep='first')
            duplicates_removed = before_dedup - len(df_cleaned)
            self.stats['duplicates_removed'] += duplicates_removed
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicate records")
        if 'dataValue' in df_cleaned.columns:
            df_cleaned = df_cleaned.dropna(subset=['dataValue'])
            df_cleaned = df_cleaned[df_cleaned['dataValue'].abs() < 999.999]
        if 'latitude' in df_cleaned.columns:
            df_cleaned = df_cleaned[(df_cleaned['latitude'] >= -90) & (df_cleaned['latitude'] <= 90)]
        if 'longitude' in df_cleaned.columns:
            df_cleaned = df_cleaned[(df_cleaned['longitude'] >= -180) & (df_cleaned['longitude'] <= 180)]
        df_cleaned = df_cleaned.where(pd.notnull(df_cleaned), None)
        final_count = len(df_cleaned)
        self.logger.info(f"Transform completed: {original_count} → {final_count} records")
        return df_cleaned

    def load_batch_to_oracle(self, batch_data, batch_id):
        if not batch_data:
            return 0
        try:
            df_batch = pd.DataFrame(batch_data)
            df_batch['batch_id'] = batch_id
            insert_sql = f"""
            INSERT INTO {self.table_name} (
                STATION_CODE, STATION_NAME, STATION_TYPE, LATITUDE, LONGITUDE,
                AGENCY_NAME, STATE, DISTRICT, DATA_ACQUISITION_MODE, STATION_STATUS,
                TEHSIL, DATATYPE_CODE, DATA_VALUE, DATA_TIME, UNIT,
                WELL_TYPE, WELL_DEPTH, WELL_AQUIFER_TYPE, BATCH_ID
            ) VALUES (
                :stationCode, :stationName, :stationType, :latitude, :longitude,
                :agencyName, :state, :district, :dataAcquisitionMode, :stationStatus,
                :tehsil, :datatypeCode, :dataValue, :dataTime, :unit,
                :wellType, :wellDepth, :wellAquiferType, :batch_id
            )
            """
            insert_data = []
            for _, row in df_batch.iterrows():
                insert_data.append({
                    'stationCode': row.get('stationCode', None),
                    'stationName': row.get('stationName', None),
                    'stationType': row.get('stationType', None),
                    'latitude': float(row['latitude']) if pd.notna(row.get('latitude')) else None,
                    'longitude': float(row['longitude']) if pd.notna(row.get('longitude')) else None,
                    'agencyName': row.get('agencyName', None),
                    'state': row.get('state', None),
                    'district': row.get('district', None),
                    'dataAcquisitionMode': row.get('dataAcquisitionMode', None),
                    'stationStatus': row.get('stationStatus', None),
                    'tehsil': row.get('tehsil', None),
                    'datatypeCode': row.get('datatypeCode', None),
                    'dataValue': float(row['dataValue']) if pd.notna(row.get('dataValue')) else None,
                    'dataTime': row.get('dataTime', None),
                    'unit': row.get('unit', None),
                    'wellType': row.get('wellType', None),
                    'wellDepth': float(row['wellDepth']) if pd.notna(row.get('wellDepth')) else None,
                    'wellAquiferType': row.get('wellAquiferType', None),
                    'batch_id': batch_id
                })
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), insert_data)
                conn.commit()
            self.stats['db_insert_batches'] += 1
            self.stats['db_total_inserted'] += len(df_batch)
            print(f"✅ Inserted batch {batch_id}: {len(df_batch)} records to Oracle")
            return len(df_batch)
        except Exception as e:
            self.logger.error(f"Failed to insert batch {batch_id} to Oracle: {e}")
            return 0

    def etl_to_oracle_parallel_single_date(self, executor, batch_size):
        tasks = []
        for state, districts in STATE_DISTRICT_MAPPING.items():
            for district in districts:
                tasks.append((state, district))

        self.stats['total_districts'] = len(tasks)
        batch_buffer = []
        batch_counter = 1

        futures = {
            executor.submit(self.extract_and_transform_district, state, district): (state, district)
            for state, district in tasks
        }

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(tasks), desc="🔄 ETL Processing"):
            state, district = futures[future]
            try:
                status, raw_count, cleaned_count, cleaned_records = future.result()
                if status == 'success':
                    self.stats['successful_districts'] += 1
                    self.stats['total_raw_records'] += raw_count
                    self.stats['total_cleaned_records'] += cleaned_count
                    if cleaned_records:
                        batch_buffer.extend(cleaned_records)
                    if len(batch_buffer) >= batch_size:
                        batch_id = f"BATCH_{batch_counter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        self.load_batch_to_oracle(batch_buffer, batch_id)
                        batch_buffer = []
                        batch_counter += 1
                else:
                    self.stats['failed_districts'] += 1
            except Exception as e:
                self.logger.error(f"ETL future failed: {e}")
                self.stats['failed_districts'] += 1

        if batch_buffer:
            batch_id = f"BATCH_{batch_counter}_FINAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.load_batch_to_oracle(batch_buffer, batch_id)

    def run_etl_over_date_range(self, start_date_str, end_date_str, batch_size=1000, delete_existing=False):
        if not self.setup_oracle_connection():
            print("❌ Failed to connect to Oracle. Aborting.")
            return
    
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

        current_date = start_date
        while current_date <= end_date:
            self.target_date = current_date.strftime("%Y-%m-%d")
            print(f"\n🗓️ Processing data for date: {self.target_date}")

            if delete_existing:
                print(f"🗑️ Deleting existing data for {self.target_date}")
                self.delete_by_date(self.target_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                self.etl_to_oracle_parallel_single_date(executor, batch_size)

            current_date += timedelta(days=1)


    def print_statistics(self):
        print("\n📊 ENHANCED ORACLE ETL PIPELINE STATISTICS:")
        print("-" * 60)
        print(f"🏛️  Districts processed: {self.stats['successful_districts']}/{self.stats['total_districts']}")
        print(f"❌ Failed districts: {self.stats['failed_districts']}")
        print(f"🔄 API retries performed: {self.stats['api_retries']}")
        if self.stats['records_deleted'] > 0:
            print(f"🗑️  Records deleted: {self.stats['records_deleted']:,}")
        if self.stats['total_districts'] > 0:
            success_rate = (self.stats['successful_districts'] / self.stats['total_districts'] * 100)
            print(f"✅ Success rate: {success_rate:.2f}%")
        print(f"\n📈 Data Processing:")
        print(f"📥 Raw records extracted: {self.stats['total_raw_records']:,}")
        print(f"🧹 Cleaned records: {self.stats['total_cleaned_records']:,}")
        print(f"🔍 Duplicates removed: {self.stats['duplicates_removed']:,}")
        print(f"💾 Records inserted to Oracle: {self.stats['db_total_inserted']:,}")
        print(f"📦 Database insert batches: {self.stats['db_insert_batches']}")

    def verify_oracle_data(self):
        try:
            with self.engine.connect() as conn:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                total_rows = count_result.fetchone()[0]
                if total_rows > 0:
                    states_result = conn.execute(text(f"SELECT COUNT(DISTINCT state) FROM {self.table_name}"))
                    states_count = states_result.fetchone()[0]
                    districts_result = conn.execute(text(f"SELECT COUNT(DISTINCT district) FROM {self.table_name}"))
                    districts_count = districts_result.fetchone()[0]
                    print(f"\n✅ ORACLE DATABASE VERIFICATION")
                    print(f"📊 Total rows in {self.table_name}: {total_rows:,}")
                    print(f"🗺️  States covered: {states_count}")
                    print(f"🏛️  Districts covered: {districts_count}")
                else:
                    print(f"\n❌ No data found in {self.table_name}")
                return total_rows
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            return 0

# Oracle Database Configuration
oracle_config = {
    'username': 'C##GROUNDWATER_DB',
    'password': 'groundwater2025',
    'host': 'localhost',
    'port': '1521',
    'service_name': 'XE'
}

def main():
    print("🇮🇳 ENHANCED INDIA GROUNDWATER ETL TO ORACLE")
    print("Complete Extract -> Transform -> Load Pipeline")
    print("Features: Duplicate Detection, API Retry, Limited Threading, Date Range Processing")
    print("=" * 70)

    oracle_etl = OracleETLGroundwaterCollector(oracle_config)

    # Run ETL pipeline for a date range, deleting existing data for each date before insertion
    oracle_etl.run_etl_over_date_range(
        start_date_str='2025-01-01',
        end_date_str='2025-01-03',   # Adjust your desired end date
        batch_size=1000,
        delete_existing=True
    )

    total_inserted = oracle_etl.verify_oracle_data()
    print(f"\n🎉 ENHANCED ORACLE ETL PIPELINE SUCCESS!")
    print(f"\n✅ Pipeline completed successfully!")
    print(f"📊 Total records in Oracle: {total_inserted:,}")
    print(f"🔍 Duplicates removed: {oracle_etl.stats['duplicates_removed']:,}")
    print(f"🔄 API retries performed: {oracle_etl.stats['api_retries']}")

if __name__ == "__main__":
    # your execution code here
    main()
