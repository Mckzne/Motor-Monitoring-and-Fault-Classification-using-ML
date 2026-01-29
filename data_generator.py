"""
PMSM Fault Diagnosis - Data Generator
Picks random sample from random fault CSV file and pushes to Firebase every 1 minute
"""

import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import random
import time
import os
from datetime import datetime

# Initialize Firebase Admin SDK
cred = credentials.Certificate('pmsm-25905-firebase-adminsdk-fbsvc-eb25d9aa87.json')  # Your key file
firebase_admin.initialize_app(cred)
db = firestore.client()

# Define the assets directory
ASSETS_DIR = '.'

# List of fault CSV files (exclude dataset.csv as it's for training only)
CSV_FILES = [
    'NORMAL_OP.csv',
    'HB1_OVER_TEMP.csv',
    'HB2_HIGH_SIDE_SC.csv',
    'HB2_HIGH_SIDE_OC.csv',
    'HB3_OVER_TEMP.csv',
    'HB1_LOW_SIDE_SC.csv',
    'HB3_LOW_SIDE_OC.csv',
    'HB12_OVER_TEMP.csv',
    'HB3_HIGH_SIDE_SC.csv',
]

# The 8 sensor features (exclude FDD column)
# The 8 sensor features (exclude FDD column)
FEATURE_COLS = ['Ia', 'Ib', 'VDC', 'IDC', 'T1', 'T2', 'T3', 'VD']


def load_csv_files():
    """Load all CSV files into memory"""
    data_dict = {}
    for csv_file in CSV_FILES:
        file_path = os.path.join(ASSETS_DIR, csv_file)
        try:
            df = pd.read_csv(file_path)
            data_dict[csv_file] = df
            print(f"Loaded {csv_file}: {len(df)} rows")
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")
    return data_dict

def get_random_sample(data_dict):
    """Select random CSV file and random row from it (exclude FDD column)"""
    csv_file = random.choice(list(data_dict.keys()))
    df = data_dict[csv_file]
    
    # Select random row
    random_idx = random.randint(0, len(df) - 1)
    row = df.iloc[random_idx]
    
    # Extract only the 8 features (skip FDD)
    sample = {col: float(row[col]) for col in FEATURE_COLS if col in row}
    sample['source_file'] = csv_file
    sample['timestamp'] = firestore.SERVER_TIMESTAMP
    
    return sample

def push_data(data_dict, interval=60):
    """Push one random sample to Firestore every interval seconds (default: 60 = 1 minute)"""
    print(f"\n Starting data stream (pushing every {interval}s / {interval/60} minute(s))...")
    print("Press Ctrl+C to stop\n")
    
    try:
        count = 0
        while True:
            # Get random sample
            sample = get_random_sample(data_dict)
            
            # Push to Firestore
            db.collection('raw_readings').add(sample)
            count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] #{count} Pushed sample from {sample['source_file']}")
            print(f"   Features: Ia={sample['Ia']:.2f}, Ib={sample['Ib']:.2f}, VDC={sample['VDC']:.2f}")

            
            # Wait before next sample
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n✓ Data stream stopped. Total samples sent: {count}")
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == '__main__':
    print("="*70)
    print("PMSM FAULT DIAGNOSIS - DATA GENERATOR".center(70))
    print("="*70)
    
    # Load all CSV files
    print("\n Loading CSV files...")
    data_dict = load_csv_files()
    
    if not data_dict:
        print("\n ERROR: No CSV files loaded. Check assets directory.")
    else:
        print(f"\n Successfully loaded {len(data_dict)} CSV files")
        
        # Start pushing samples every 1 minute
        push_data(data_dict, interval=60)
