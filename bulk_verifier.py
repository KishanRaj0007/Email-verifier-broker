import csv
import requests
import sys

def run_bulk_verification(input_filename="team_data.csv"):
    print(f"Reading data from {input_filename}...")
    
    contacts = []
    
    try:
        with open(input_filename, mode='r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            
            for row in reader:
                # Skip empty rows or rows without an email in the first column
                if not row or not row[0].strip():
                    continue
                
                # Map columns by index assuming order: Email, Name, Phone, Score, URL
                contact = {
                    "email": row[0].strip(),
                    "name": row[1].strip() if len(row) > 1 else "",
                    "phone": row[2].strip() if len(row) > 2 else "",
                    "score": row[3].strip() if len(row) > 3 else 0,
                    "url": row[4].strip() if len(row) > 4 else ""
                }
                contacts.append(contact)
                
    except FileNotFoundError:
        print(f"Error: Could not find '{input_filename}'.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    if not contacts:
        print("No valid emails found in the CSV.")
        sys.exit(1)

    print(f"Sending {len(contacts)} contacts to the Java Microservice...")

    try:
        url = "http://localhost:8080/api/v1/verify-batch"
        response = requests.post(url, json=contacts, timeout=600) 
        
        if response.status_code == 200:
            verified_results = response.json()
        else:
            print(f"Java Server Error: HTTP {response.status_code}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Connection failed. Ensure the Java microservice is running. Error: {e}")
        sys.exit(1)

    valid_emails = [c for c in verified_results if c.get('status') in ['VALID', 'RISKY_CATCH_ALL']]
    dead_emails = [c for c in verified_results if c.get('status') in ['DEAD', 'ERROR']]
    
    print("\nVerification Complete.")
    print(f"Valid/Risky: {len(valid_emails)}")
    print(f"Dead/Bounces: {len(dead_emails)}")

    if valid_emails:
        save_to_csv(valid_emails, "verified_clean_list.csv")
    if dead_emails:
        save_to_csv(dead_emails, "verified_dead_list.csv")

def save_to_csv(data, filename):
    if not data:
        return
        
    with open(filename, mode='w', newline='', encoding='utf-8-sig') as file:
        fieldnames = list(data[0].keys())
        if 'status' not in fieldnames:
            fieldnames.append('status')
            
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Writes a header in the final output file for clarity
        writer.writeheader()
        writer.writerows(data)
        
    print(f"Saved {len(data)} records to {filename}")

if __name__ == "__main__":
    run_bulk_verification("team_data.csv")