import requests  # pentru efectuarea de cereri HTTP
from bs4 import BeautifulSoup  # pentru analiza și extragerea de date din HTML
import pandas as pd  # pentru lucrul cu seturi de date
import re  # pentru expresii regulate

# Încarcă setul de date din fișierul parquet
df = pd.read_parquet('list of company websites.snappy.parquet')

# Definește antetele pentru cererile HTTP
headers = {'User-Agent': 'Mozilla/5.0'}

# Definește modelul de expresie regulată pentru adrese
address_pattern = r'(\d+)\s([a-zA-Z\s\.\#]+)\,\s([A-Za-z]+)\s(\d{5})|\d+\s([A-Za-z\s\.\#]+)\,\sSuite\s(\d+)([A-Za-z\s]*)\,\s([A-Za-z]+)\s(\d{5})'

# Lista cu coduri de eroare HTTP comune
error_codes = {'400', '401', '403', '404', '500', '503'}

# Inițializează lista pentru a stoca rezultatele
results = []

# Creează o sesiune de cereri HTTP
with requests.Session() as session:
    for domain in df['domain']:
        try:
            # Realizează cererea către domeniul specificat
            response = session.get('https://' + domain, headers=headers, timeout=10)

            # Verifică dacă răspunsul conține un cod de eroare HTTP
            if str(response.status_code) in error_codes:
                results.append({
                    'Domain': domain,
                    'Details': f"Error: HTTP {response.status_code} - {response.reason}"
                })
                continue

            # Analizează conținutul HTML folosind BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # Verifică dacă există un element body în pagina web
            if soup.body:
                text = soup.body.get_text(separator=' ', strip=True)
            else:
                text = soup.get_text(separator=' ', strip=True)

            # Extrage adresa folosind expresia regulată
            address_matches = re.findall(address_pattern, text, re.IGNORECASE)

            if address_matches:
                address_details = address_matches[0]
                # Verifică formatul adresei și stochează detaliile corespunzătoare
                if address_details[0]:
                    street_number, street_name, city, postcode = address_details[0], address_details[1].strip(), \
                        address_details[2], address_details[3]
                else:
                    street_name, street_number, city, postcode = address_details[4], address_details[5].strip(), \
                        address_details[7], address_details[8]

                results.append({
                    'Domain': domain,
                    'Country': 'USA',
                    'Region': city[:2],  # Primele două caractere ale orașului ca regiune
                    'City': city,
                    'Postcode': postcode,
                    'Road': street_name,
                    'Road Numbers': street_number,
                    'Details': ''
                })
            else:
                results.append({
                    'Domain': domain,
                    'Details': 'No address found'
                })

        # Gestionarea excepțiilor pentru cererile HTTP sau decodarea textului
        except (requests.exceptions.RequestException, UnicodeDecodeError) as e:
            results.append({
                'Domain': domain,
                'Details': f"Error: {str(e)}"
            })

# Creează un DataFrame din lista de rezultate
df_results = pd.DataFrame(results)

# Salvează DataFrame-ul într-un fișier Excel
df_results.to_excel('company_details.xlsx', index=False)

# Afișează confirmarea
print("DataFrame saved to 'company_details.xlsx'")
