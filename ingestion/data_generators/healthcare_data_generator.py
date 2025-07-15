"""
Healthcare Data Generator for Synthetic Data Creation
Generates realistic healthcare data for IoT devices, claims, and EHR systems.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from faker import Faker
import numpy as np

fake = Faker()

@dataclass
class PatientVitals:
    """Patient vital signs data structure"""
    patient_id: str
    timestamp: str
    heart_rate: int
    blood_pressure_systolic: int
    blood_pressure_diastolic: int
    temperature: float
    oxygen_saturation: int
    respiratory_rate: int
    device_id: str
    location: str

@dataclass
class InsuranceClaim:
    """Insurance claim data structure"""
    claim_id: str
    patient_id: str
    provider_id: str
    service_date: str
    diagnosis_codes: List[str]
    procedure_codes: List[str]
    total_amount: float
    insurance_type: str
    claim_status: str
    submission_date: str

@dataclass
class EHRRecord:
    """Electronic Health Record data structure"""
    record_id: str
    patient_id: str
    visit_date: str
    provider_id: str
    diagnosis: str
    treatment: str
    medications: List[str]
    lab_results: Dict[str, Any]
    notes: str

class HealthcareDataGenerator:
    """Generates synthetic healthcare data for various sources"""
    
    def __init__(self):
        self.patient_ids = [f"P{str(i).zfill(6)}" for i in range(1, 1001)]
        self.provider_ids = [f"DR{str(i).zfill(4)}" for i in range(1, 101)]
        self.device_ids = [f"DEV{str(i).zfill(4)}" for i in range(1, 51)]
        self.locations = ["ICU", "Emergency", "General Ward", "Operating Room", "Recovery Room"]
        
        # Diagnosis codes (ICD-10)
        self.diagnosis_codes = [
            "I21.9", "I50.9", "E11.9", "J44.9", "N18.9",  # Heart, Diabetes, COPD, Kidney
            "I63.9", "I10", "E78.5", "J45.909", "K76.0"   # Stroke, Hypertension, etc.
        ]
        
        # Procedure codes (CPT)
        self.procedure_codes = [
            "99213", "99214", "99215", "99223", "99224",  # Office visits
            "93010", "71046", "80048", "84443", "85025"   # Tests and procedures
        ]
        
        # Insurance types
        self.insurance_types = ["Medicare", "Medicaid", "Private", "Self-Pay"]
        
        # Claim statuses
        self.claim_statuses = ["Submitted", "Under Review", "Approved", "Denied", "Paid"]
        
        # Medications
        self.medications = [
            "Aspirin", "Lisinopril", "Metformin", "Atorvastatin", "Amlodipine",
            "Omeprazole", "Albuterol", "Warfarin", "Furosemide", "Metoprolol"
        ]
        
        # Lab tests
        self.lab_tests = {
            "CBC": {"unit": "cells/μL", "normal_range": (4000, 11000)},
            "Glucose": {"unit": "mg/dL", "normal_range": (70, 100)},
            "Creatinine": {"unit": "mg/dL", "normal_range": (0.6, 1.2)},
            "Cholesterol": {"unit": "mg/dL", "normal_range": (125, 200)},
            "Hemoglobin": {"unit": "g/dL", "normal_range": (12, 16)}
        }
    
    def generate_patient_vitals(self, patient_id: str = None) -> PatientVitals:
        """Generate realistic patient vital signs"""
        if patient_id is None:
            patient_id = random.choice(self.patient_ids)
        
        # Generate realistic vital signs with some correlation
        base_heart_rate = random.randint(60, 100)
        base_temp = random.uniform(36.5, 37.5)
        
        # Add some variability and correlation
        heart_rate = max(40, min(180, base_heart_rate + random.randint(-10, 10)))
        temp = max(35.0, min(40.0, base_temp + random.uniform(-0.5, 0.5)))
        
        # Blood pressure (systolic/diastolic)
        systolic = random.randint(90, 180)
        diastolic = max(60, min(systolic - 20, random.randint(60, 100)))
        
        # Oxygen saturation (should be high for most patients)
        oxygen_sat = random.randint(95, 100)
        
        # Respiratory rate
        resp_rate = random.randint(12, 20)
        
        return PatientVitals(
            patient_id=patient_id,
            timestamp=datetime.now().isoformat(),
            heart_rate=heart_rate,
            blood_pressure_systolic=systolic,
            blood_pressure_diastolic=diastolic,
            temperature=round(temp, 1),
            oxygen_saturation=oxygen_sat,
            respiratory_rate=resp_rate,
            device_id=random.choice(self.device_ids),
            location=random.choice(self.locations)
        )
    
    def generate_insurance_claim(self, patient_id: str = None) -> InsuranceClaim:
        """Generate realistic insurance claim data"""
        if patient_id is None:
            patient_id = random.choice(self.patient_ids)
        
        service_date = fake.date_between(start_date='-30d', end_date='today')
        submission_date = fake.date_between(start_date=service_date, end_date='today')
        
        # Generate diagnosis and procedure codes
        num_diagnoses = random.randint(1, 3)
        num_procedures = random.randint(1, 2)
        
        diagnosis_codes = random.sample(self.diagnosis_codes, num_diagnoses)
        procedure_codes = random.sample(self.procedure_codes, num_procedures)
        
        # Generate realistic claim amount
        base_amount = random.randint(100, 2000)
        total_amount = round(base_amount * random.uniform(0.8, 1.2), 2)
        
        return InsuranceClaim(
            claim_id=f"CLM{str(random.randint(100000, 999999))}",
            patient_id=patient_id,
            provider_id=random.choice(self.provider_ids),
            service_date=service_date.strftime("%Y-%m-%d"),
            diagnosis_codes=diagnosis_codes,
            procedure_codes=procedure_codes,
            total_amount=total_amount,
            insurance_type=random.choice(self.insurance_types),
            claim_status=random.choice(self.claim_statuses),
            submission_date=submission_date.strftime("%Y-%m-%d")
        )
    
    def generate_ehr_record(self, patient_id: str = None) -> EHRRecord:
        """Generate realistic EHR record"""
        if patient_id is None:
            patient_id = random.choice(self.patient_ids)
        
        visit_date = fake.date_between(start_date='-90d', end_date='today')
        
        # Generate lab results
        lab_results = {}
        for test_name, test_info in self.lab_tests.items():
            normal_min, normal_max = test_info["normal_range"]
            # 80% chance of normal results, 20% chance of abnormal
            if random.random() < 0.8:
                value = random.uniform(normal_min, normal_max)
            else:
                # Generate abnormal value
                if random.random() < 0.5:
                    value = random.uniform(normal_min * 0.5, normal_min)
                else:
                    value = random.uniform(normal_max, normal_max * 1.5)
            
            lab_results[test_name] = {
                "value": round(value, 2),
                "unit": test_info["unit"],
                "normal_range": f"{normal_min}-{normal_max}"
            }
        
        # Generate medications (1-3 medications per visit)
        num_medications = random.randint(1, 3)
        medications = random.sample(self.medications, num_medications)
        
        return EHRRecord(
            record_id=f"EHR{str(random.randint(100000, 999999))}",
            patient_id=patient_id,
            visit_date=visit_date.strftime("%Y-%m-%d"),
            provider_id=random.choice(self.provider_ids),
            diagnosis=fake.sentence(nb_words=6),
            treatment=fake.sentence(nb_words=8),
            medications=medications,
            lab_results=lab_results,
            notes=fake.text(max_nb_chars=200)
        )
    
    def generate_batch_data(self, data_type: str, count: int = 100) -> List[Dict[str, Any]]:
        """Generate batch data for a specific type"""
        data = []
        
        for _ in range(count):
            if data_type == "vitals":
                record = self.generate_patient_vitals()
            elif data_type == "claims":
                record = self.generate_insurance_claim()
            elif data_type == "ehr":
                record = self.generate_ehr_record()
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
            data.append(asdict(record))
        
        return data
    
    def generate_streaming_data(self, data_type: str, duration_minutes: int = 5) -> List[Dict[str, Any]]:
        """Generate streaming data for a specific duration"""
        data = []
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        while datetime.now() < end_time:
            if data_type == "vitals":
                record = self.generate_patient_vitals()
            elif data_type == "claims":
                record = self.generate_insurance_claim()
            elif data_type == "ehr":
                record = self.generate_ehr_record()
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
            data.append(asdict(record))
            time.sleep(random.uniform(0.1, 1.0))  # Random interval between records
        
        return data

def main():
    """Example usage of the data generator"""
    generator = HealthcareDataGenerator()
    
    print("Generating sample healthcare data...")
    
    # Generate sample vitals
    vitals = generator.generate_patient_vitals()
    print(f"Sample Vitals: {json.dumps(asdict(vitals), indent=2)}")
    
    # Generate sample claim
    claim = generator.generate_insurance_claim()
    print(f"Sample Claim: {json.dumps(asdict(claim), indent=2)}")
    
    # Generate sample EHR record
    ehr = generator.generate_ehr_record()
    print(f"Sample EHR: {json.dumps(asdict(ehr), indent=2)}")
    
    # Generate batch data
    batch_vitals = generator.generate_batch_data("vitals", 5)
    print(f"Batch Vitals (5 records): {json.dumps(batch_vitals, indent=2)}")

if __name__ == "__main__":
    main() 